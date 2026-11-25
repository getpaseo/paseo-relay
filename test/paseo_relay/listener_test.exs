defmodule PaseoRelay.ListenerTest do
  use ExUnit.Case, async: false

  import Bitwise

  @capacity_mutation_timeout_ms PaseoRelay.Config.defaults().capacity_mutation_timeout_ms

  setup do
    assert PaseoRelay.Metrics.value(:active_websockets) == 0

    on_exit(fn ->
      assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end)
    end)

    :ok
  end

  test "the native Cowboy listener serves relay operations" do
    listener = {:listener_test, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: listener,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 10}
    )

    # The kernel sizes the receive buffer. The user-space buffer is fixed.
    socket_options = :ranch.get_transport_options(listener).socket_opts
    assert socket_options[:buffer] == 65_536
    refute Keyword.has_key?(socket_options, :recbuf)

    port = PaseoRelay.Listener.port(listener)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, "GET /health HTTP/1.1\r\nHost: relay.test\r\n\r\n")
    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)

    assert response =~ "HTTP/1.1 200 OK"
    assert response =~ ~s({"status":"ok"})
  end

  test "an explicit receive buffer does not raise the user-space buffer of a socket" do
    listener = {:listener_receive_buffer, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: listener,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 10,
       receive_buffer_bytes: 1_048_576}
    )

    assert :ranch.get_transport_options(listener).socket_opts[:recbuf] == 1_048_576

    port = PaseoRelay.Listener.port(listener)
    assert http_get(port, "/health") =~ "HTTP/1.1 200 OK"
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(client) end)
    assert_eventually(fn -> length(:ranch.procs(listener, :connections)) == 1 end)

    [connection] = :ranch.procs(listener, :connections)
    {:links, links} = Process.info(connection, :links)
    socket = Enum.find(links, &is_port/1)

    # The driver allocates this buffer for each socket that reads.
    assert :inet.getopts(socket, [:buffer]) == {:ok, [buffer: 65_536]}
  end

  test "a stalled HTTP body releases its listener slot without expiring WebSockets" do
    listener = {:listener_http_idle, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: listener,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       connection_supervisors: 1,
       max_connections: 1,
       http_idle_timeout_ms: 100}
    )

    port = PaseoRelay.Listener.port(listener)
    {:ok, stalled} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    on_exit(fn ->
      :gen_tcp.close(stalled)
    end)

    :ok =
      :gen_tcp.send(
        stalled,
        "POST /health HTTP/1.1\r\nHost: relay.test\r\nContent-Length: 1\r\n\r\n"
      )

    assert {:ok, stalled_response} = :gen_tcp.recv(stalled, 0, 2_000)
    assert stalled_response =~ "HTTP/1.1 200 OK"

    {:ok, health} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(health) end)
    :ok = :gen_tcp.send(health, "GET /health HTTP/1.1\r\nHost: relay.test\r\n\r\n")
    assert {:ok, response} = :gen_tcp.recv(health, 0, 2_000)
    assert response =~ "HTTP/1.1 200 OK"
    :ok = :gen_tcp.close(health)

    websocket = open_websocket(port, "http-timeout-websocket")
    on_exit(fn -> :gen_tcp.close(websocket) end)
    Process.sleep(200)
    :ok = :gen_tcp.send(websocket, :cow_ws.masked_frame({:ping, "alive"}, 0x10203040))
    assert {:pong, "alive"} = recv_until_pong(websocket, "alive")
  end

  test "the active WebSocket ceiling rejects exactly at capacity and releases on close" do
    reference = {:listener_budget, System.unique_integer([:positive])}
    rejections = PaseoRelay.Metrics.value(:connection_rejections)

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 2,
       max_connections: 100,
       max_websockets: 2}
    )

    port = PaseoRelay.Listener.port(reference)
    rejected_server_id = "budget-third-#{System.unique_integer([:positive])}"
    first = open_websocket(port, "budget-first")
    second = open_websocket(port, "budget-second")

    assert PaseoRelay.Capacity.active_connections(reference) == 2
    assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"

    rejected = connect_and_request_websocket(port, rejected_server_id)
    assert {:ok, response} = :gen_tcp.recv(rejected, 0, 2_000)
    assert response =~ "HTTP/1.1 503 Service Unavailable"
    assert response =~ "Relay connection capacity"
    assert PaseoRelay.Metrics.value(:connection_rejections) == rejections + 1
    assert PaseoRelay.Ownership.resolve(rejected_server_id) == :unowned
    :ok = :gen_tcp.close(rejected)

    :ok = :gen_tcp.close(first)
    assert_eventually(fn -> PaseoRelay.Capacity.active_connections(reference) == 1 end)
    assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"

    replacement = open_websocket(port, rejected_server_id)
    assert PaseoRelay.Capacity.active_connections(reference) == 2

    :ok = :gen_tcp.close(second)
    :ok = :gen_tcp.close(replacement)
    assert_eventually(fn -> PaseoRelay.Capacity.active_connections(reference) == 0 end)
  end

  @tag timeout: 15_000
  test "a timed-out upgrade invalidates its Capacity and listener epoch" do
    reference = PaseoRelay.Listener
    port = PaseoRelay.Listener.port(reference)
    initial_connections = MapSet.new(:ranch.procs(reference, :connections))
    established = open_websocket(port, "stalled-capacity-existing")

    assert_eventually(fn ->
      reference
      |> :ranch.procs(:connections)
      |> MapSet.new()
      |> MapSet.difference(initial_connections)
      |> MapSet.size() == 1
    end)

    [established_connection] =
      reference
      |> :ranch.procs(:connections)
      |> MapSet.new()
      |> MapSet.difference(initial_connections)
      |> MapSet.to_list()

    established_monitor = Process.monitor(established_connection)
    capacity = Process.whereis(PaseoRelay.Capacity)
    listener = runtime_child(PaseoRelay.Listener)
    existing_connections = MapSet.new(:ranch.procs(reference, :connections))
    capacity_monitor = Process.monitor(capacity)
    listener_monitor = Process.monitor(listener)
    :ok = :sys.suspend(capacity)

    on_exit(fn ->
      if Process.alive?(capacity), do: :sys.resume(capacity)
    end)

    socket = connect_and_request_websocket(port, "stalled-capacity-command")

    assert_eventually(fn ->
      reference
      |> :ranch.procs(:connections)
      |> MapSet.new()
      |> MapSet.difference(existing_connections)
      |> MapSet.size() == 1
    end)

    [connection] =
      reference
      |> :ranch.procs(:connections)
      |> MapSet.new()
      |> MapSet.difference(existing_connections)
      |> MapSet.to_list()

    connection_monitor = Process.monitor(connection)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 4_500)
    assert_receive {:DOWN, ^capacity_monitor, :process, ^capacity, :killed}, 2_000
    assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :shutdown}, 2_000

    assert_receive {:DOWN, ^established_monitor, :process, ^established_connection,
                    _established_reason},
                   2_000

    assert_receive {:DOWN, ^connection_monitor, :process, ^connection, _stalled_reason}, 2_000
    assert_eventually(fn -> Process.whereis(PaseoRelay.Capacity) != capacity end)
    assert_eventually(fn -> runtime_child(PaseoRelay.Listener) != listener end)
    assert PaseoRelay.Capacity.active_connections(reference) == 0
    assert PaseoRelay.Metrics.value(:active_websockets) == 0
    assert PaseoRelay.Ownership.resolve("stalled-capacity-command") == :unowned

    replacement = open_websocket(port, "stalled-capacity-command")

    :ok = :gen_tcp.close(socket)
    :ok = :gen_tcp.close(established)
    :ok = :gen_tcp.close(replacement)
    assert_eventually(fn -> PaseoRelay.Capacity.active_connections(reference) == 0 end)
    assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end)
  end

  @tag timeout: 5_000
  test "a caller disconnect during stalled admission leaves no public reservation" do
    reference = {:listener_stalled_disconnect, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 10,
       max_websockets: 1}
    )

    capacity = Process.whereis(PaseoRelay.Capacity)
    :ok = :sys.suspend(capacity)

    on_exit(fn ->
      if Process.alive?(capacity), do: :sys.resume(capacity)
    end)

    socket =
      reference
      |> PaseoRelay.Listener.port()
      |> connect_and_request_websocket("stalled-capacity-disconnect")

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 100)
    :ok = :gen_tcp.close(socket)
    assert_eventually(fn -> :ranch.procs(reference, :connections) == [] end)

    :ok = :sys.resume(capacity)

    assert Process.whereis(PaseoRelay.Capacity) == capacity
    assert PaseoRelay.Capacity.active_connections(reference) == 0

    replacement =
      reference
      |> PaseoRelay.Listener.port()
      |> open_websocket("stalled-capacity-replacement")

    :ok = :gen_tcp.close(replacement)
    assert_eventually(fn -> PaseoRelay.Capacity.active_connections(reference) == 0 end)
    assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end)
  end

  @tag timeout: 8_000
  test "a timed-out watermark mutation invalidates its Capacity epoch" do
    capacity = Process.whereis(PaseoRelay.Capacity)
    capacity_monitor = Process.monitor(capacity)
    :ok = :sys.suspend(capacity)

    on_exit(fn ->
      if Process.alive?(capacity), do: :sys.resume(capacity)
      PaseoRelay.Capacity.set_watermark(0, @capacity_mutation_timeout_ms)
    end)

    control =
      Task.async(fn ->
        PaseoRelay.Capacity.set_watermark(1, @capacity_mutation_timeout_ms)
      end)

    assert Task.await(control, 6_000) == {:error, :unavailable}
    assert_receive {:DOWN, ^capacity_monitor, :process, ^capacity, :killed}, 2_000

    assert_eventually(fn ->
      replacement = Process.whereis(PaseoRelay.Capacity)
      is_pid(replacement) and replacement != capacity
    end)

    replacement = Process.whereis(PaseoRelay.Capacity)
    assert is_pid(replacement)

    assert {:available, %{admission: :open}} =
             PaseoRelay.Capacity.status(PaseoRelay.Listener, 20_000)
  end

  defp runtime_child(id) do
    PaseoRelay.RuntimeSupervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^id, process, _type, _modules} -> process
      _other -> nil
    end)
  end

  test "the active WebSocket gauge reconciles when the heap fuse kills a socket" do
    reference = {:listener_heap_fuse, System.unique_integer([:positive])}
    config = %{PaseoRelay.Config.defaults() | websocket_max_heap_words: 65_536}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: config,
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 100,
       max_websockets: 2}
    )

    socket = open_websocket(PaseoRelay.Listener.port(reference), "heap-fuse")
    assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 1 end)

    payload = :binary.copy(<<0x5A>>, 1024 * 1024)
    :ok = :gen_tcp.send(socket, :cow_ws.masked_frame({:binary, payload}, 0x11223344))

    assert_eventually(fn -> PaseoRelay.Capacity.active_connections(reference) == 0 end)
    assert PaseoRelay.Metrics.value(:active_websockets) == 0
  end

  test "a queued reservation expiry cannot release an attached connection" do
    namespace = {:stale_expiry, System.unique_integer([:positive])}

    assert {:ok, token} =
             PaseoRelay.Capacity.admit_connection(namespace, 1, @capacity_mutation_timeout_ms)

    assert {:ok, _capacity} =
             PaseoRelay.Capacity.attach_connection(token, @capacity_mutation_timeout_ms)

    send(PaseoRelay.Capacity, {:expire, token})

    assert PaseoRelay.Capacity.active_connections(namespace) == 1
    assert :ok = PaseoRelay.Capacity.release_connection(token)
    assert PaseoRelay.Capacity.active_connections(namespace) == 0
  end

  test "pressure shedding makes message admission terminal for the selected socket" do
    namespace = {:shedding_terminal, System.unique_integer([:positive])}
    watermark = PaseoRelay.Config.defaults().memory_watermark_bytes

    on_exit(fn ->
      PaseoRelay.Capacity.set_watermark(watermark, @capacity_mutation_timeout_ms)
    end)

    assert {:ok, connection} =
             PaseoRelay.Capacity.admit_connection(namespace, 1, @capacity_mutation_timeout_ms)

    assert {:ok, _capacity} =
             PaseoRelay.Capacity.attach_connection(connection, @capacity_mutation_timeout_ms)

    assert {:ok, message} =
             PaseoRelay.Capacity.admit_message(1, @capacity_mutation_timeout_ms)

    assert :ok = PaseoRelay.Capacity.set_watermark(1, @capacity_mutation_timeout_ms)
    assert :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)

    assert {:error, :closed} =
             PaseoRelay.Capacity.admit_message(1, @capacity_mutation_timeout_ms)

    assert {:error, :closed} =
             PaseoRelay.Capacity.start_delivery(message, @capacity_mutation_timeout_ms)

    assert :ok = PaseoRelay.Capacity.release_connection(connection)
  end

  test "one pressure check sheds enough real sockets for the current memory overshoot" do
    port = PaseoRelay.Listener.port(PaseoRelay.Listener)
    first = open_websocket(port, "pressure-batch-first")
    second = open_websocket(port, "pressure-batch-second")

    assert_eventually(
      fn -> PaseoRelay.Metrics.value(:active_websockets) == 2 end,
      @capacity_mutation_timeout_ms
    )

    padding = :binary.copy(<<0x4D>>, 40 * 1024 * 1024)
    maximum_message = PaseoRelay.Protocol.maximum_message_payload_bytes()
    :erlang.garbage_collect(self())
    watermark = :erlang.memory(:total) - maximum_message - 1
    assert watermark > 0

    on_exit(fn -> PaseoRelay.Capacity.set_watermark(0, @capacity_mutation_timeout_ms) end)
    assert :ok = PaseoRelay.Capacity.set_watermark(watermark, @capacity_mutation_timeout_ms)
    assert :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)
    assert byte_size(padding) == 40 * 1024 * 1024
    assert :ok = PaseoRelay.Capacity.set_watermark(0, @capacity_mutation_timeout_ms)

    assert_eventually(
      fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end,
      @capacity_mutation_timeout_ms
    )

    assert {:close, 1013, "Relay memory pressure"} = recv_until_close(first)
    assert {:close, 1013, "Relay memory pressure"} = recv_until_close(second)
  end

  describe "the OOM guard" do
    @describetag :linux

    setup do
      directory = Path.join(System.tmp_dir!(), "oom_guard_#{System.unique_integer([:positive])}")
      File.mkdir_p!(directory)

      paths = [
        meminfo: Path.join(directory, "meminfo"),
        cgroup_max: Path.join(directory, "memory.max"),
        cgroup_current: Path.join(directory, "memory.current"),
        cgroup_stat: Path.join(directory, "memory.stat")
      ]

      Application.put_env(:paseo_relay, :system_memory_paths, paths)

      on_exit(fn ->
        Application.delete_env(:paseo_relay, :system_memory_paths)
        PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)
        File.rm_rf!(directory)
      end)

      {:ok, paths: paths}
    end

    test "sheds on low available memory and recovers, without a byte limit", %{paths: paths} do
      port = PaseoRelay.Listener.port(PaseoRelay.Listener)
      # In kB, as /proc/meminfo reports it, so that each percentage is exact.
      total = 16_000_000 * 1024
      assert PaseoRelay.RuntimeConfig.current().memory_watermark_bytes == 0

      report_memory(paths, total, 20)
      socket = open_websocket(port, "oom-guard-#{System.unique_integer([:positive])}")
      assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"
      metrics = http_get(port, "/metrics")
      assert metrics =~ "paseo_relay_system_memory_total_bytes #{total}\n"
      assert metrics =~ "paseo_relay_system_memory_available_bytes #{div(total * 20, 100)}\n"
      assert metrics =~ "paseo_relay_oom_floor_bytes #{div(total * 10, 100)}\n"
      assert metrics =~ "paseo_relay_memory_watermark_bytes 0\n"

      # Below the 10% floor: the socket closes and admission stops.
      report_memory(paths, total, 9)
      assert {:close, 1013, "Relay memory pressure"} = recv_until_close(socket)
      assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"

      rejected = connect_and_request_websocket(port, "oom-guard-rejected")
      assert {:ok, response} = :gen_tcp.recv(rejected, 0, 2_000)
      assert response =~ "Relay memory pressure"
      :gen_tcp.close(rejected)

      # Above the floor but below the 15% recovery level: still closed.
      report_memory(paths, total, 12)
      assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"

      report_memory(paths, total, 15)
      assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"
      replacement = open_websocket(port, "oom-guard-#{System.unique_integer([:positive])}")
      :gen_tcp.close(replacement)
      :gen_tcp.close(socket)
    end

    test "uses the container limit when it is below the host memory", %{paths: paths} do
      port = PaseoRelay.Listener.port(PaseoRelay.Listener)
      limit = 2 * 1024 * 1024 * 1024

      report_memory(paths, 16 * 1024 * 1024 * 1024, 80)
      File.write!(paths[:cgroup_max], "#{limit}\n")
      File.write!(paths[:cgroup_current], "#{div(limit * 95, 100)}\n")
      :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)

      assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"
      assert http_get(port, "/metrics") =~ "paseo_relay_system_memory_total_bytes #{limit}\n"

      File.write!(paths[:cgroup_current], "#{div(limit, 2)}\n")
      :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)
      assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"
    end

    test "a 0 floor and an explicit BEAM watermark each replace it", %{paths: paths} do
      port = PaseoRelay.Listener.port(PaseoRelay.Listener)
      live = PaseoRelay.RuntimeConfig.get()
      on_exit(fn -> {:ok, _settings} = PaseoRelay.RuntimeConfig.put(live) end)

      report_memory(paths, 16 * 1024 * 1024 * 1024, 5)
      assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"

      assert {:ok, _settings} = PaseoRelay.RuntimeConfig.put(%{oom_min_available_percent: 0})
      assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"
      assert http_get(port, "/metrics") =~ "paseo_relay_oom_floor_bytes 0\n"

      assert {:ok, _settings} = PaseoRelay.RuntimeConfig.put(%{oom_min_available_percent: 10})
      :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)
      assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"

      watermark = 64 * 1024 * 1024 * 1024
      assert {:ok, _settings} = PaseoRelay.RuntimeConfig.put(%{memory_watermark_bytes: watermark})
      assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"
      assert http_get(port, "/metrics") =~ "paseo_relay_oom_floor_bytes 0\n"
    end

    # Writes a host memory reading and has Capacity act on it.
    defp report_memory(paths, total_bytes, available_percent) do
      available_bytes = div(total_bytes * available_percent, 100)

      File.write!(paths[:meminfo], """
      MemTotal:       #{div(total_bytes, 1024)} kB
      MemFree:        1024 kB
      MemAvailable:   #{div(available_bytes, 1024)} kB
      """)

      :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)
    end
  end

  defp open_websocket(port, server_id) do
    socket = connect_and_request_websocket(port, server_id)
    assert {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    assert response =~ "HTTP/1.1 101 Switching Protocols"
    socket
  end

  defp connect_and_request_websocket(port, server_id) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    key = Base.encode64(:crypto.strong_rand_bytes(16))

    request =
      "GET /ws?serverId=#{server_id}&role=server&v=2 HTTP/1.1\r\n" <>
        "Host: relay.test\r\n" <>
        "Upgrade: websocket\r\n" <>
        "Connection: Upgrade\r\n" <>
        "Sec-WebSocket-Version: 13\r\n" <>
        "Sec-WebSocket-Key: #{key}\r\n\r\n"

    :ok = :gen_tcp.send(socket, request)
    socket
  end

  defp http_get(port, path) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, "GET #{path} HTTP/1.0\r\nHost: relay.test\r\n\r\n")
    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    :ok = :gen_tcp.close(socket)
    response
  end

  defp recv_until_close(socket) do
    case recv_server_frame(socket) do
      {:close, _code, _reason} = close -> close
      _frame -> recv_until_close(socket)
    end
  end

  defp recv_until_pong(socket, payload) do
    case recv_server_frame(socket) do
      {:pong, ^payload} = pong -> pong
      _frame -> recv_until_pong(socket, payload)
    end
  end

  defp recv_server_frame(socket) do
    {:ok, <<first, second>>} = :gen_tcp.recv(socket, 2, 2_000)
    opcode = first &&& 0x0F
    length = second &&& 0x7F

    length =
      case length do
        126 ->
          {:ok, <<value::16>>} = :gen_tcp.recv(socket, 2, 2_000)
          value

        127 ->
          {:ok, <<value::64>>} = :gen_tcp.recv(socket, 8, 2_000)
          value

        value ->
          value
      end

    {:ok, payload} = :gen_tcp.recv(socket, length, 2_000)

    case {opcode, payload} do
      {0x8, <<code::16, reason::binary>>} -> {:close, code, reason}
      {0x1, payload} -> {:text, payload}
      {0x2, payload} -> {:binary, payload}
      {0xA, payload} -> {:pong, payload}
    end
  end

  defp assert_eventually(assertion, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    until_true(assertion, deadline)
  end

  defp until_true(assertion, deadline) do
    if assertion.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true before timeout")
      else
        receive do
        after
          10 -> until_true(assertion, deadline)
        end
      end
    end
  end
end
