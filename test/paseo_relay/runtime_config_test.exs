defmodule PaseoRelay.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias PaseoRelay.Capacity
  alias PaseoRelay.Config
  alias PaseoRelay.Operations
  alias PaseoRelay.RuntimeConfig

  @listener PaseoRelay.Listener
  @mebibyte 1024 * 1024
  @watermark 64 * 1024 * @mebibyte

  # One valid change for every live setting. A new Config field must be added
  # here or to Config.boot_only/0.
  @changes %{
    drain: true,
    connections_per_acceptor: 600,
    http_idle_timeout_ms: 20_000,
    capacity_mutation_timeout_ms: 6_000,
    ingress_budget_bytes: 1024 * @mebibyte,
    ingress_weight: 5,
    delivery_timeout_ms: 20_000,
    transport_send_timeout_ms: 40_000,
    control_queue_bytes: 2 * @mebibyte,
    data_attach_timeout_ms: 10_000,
    tcp_user_buffer_bytes: 32_768,
    tcp_receive_buffer_bytes: 32_768,
    websocket_max_heap_words: 64 * @mebibyte,
    memory_watermark_bytes: @watermark,
    oom_min_available_percent: 5,
    oom_recovery_available_percent: 8,
    heal_enabled: false,
    heal_delay_ms: 1_234,
    heal_rate_per_sec: 7,
    heal_attempt_window_ms: 60_000,
    heal_cooldown_ms: 600_000,
    home_max_idle_ms: 86_400_000,
    ownership_target: "elsewhere",
    reroute_header: "x-other-target",
    minimum_cluster_size: 2,
    cluster_nodes: [:"relay_absent@127.0.0.1"]
  }

  setup do
    live = RuntimeConfig.get()
    on_exit(fn -> {:ok, _settings} = RuntimeConfig.put(live) end)
  end

  test "every Config field is a live setting or explicitly boot-only" do
    on_exit(&restart_listener/0)
    fields = Config.defaults() |> Map.from_struct() |> Map.keys()

    assert Enum.sort(Config.boot_only()) ==
             [:acceptors, :cluster_query, :cookie, :host, :ip, :node_name, :port]

    assert Enum.sort(Config.live_keys() ++ Config.boot_only()) == Enum.sort(fields)
    assert Enum.sort(Map.keys(@changes)) == Enum.sort(Config.live_keys())

    for {key, value} <- @changes, do: refute(Map.fetch!(RuntimeConfig.current(), key) == value)

    daemon = open_daemon(server_id("every"))
    assert RuntimeConfig.put(@changes) == {:ok, @changes}
    assert RuntimeConfig.get() == @changes
    assert Map.take(RuntimeConfig.current(), Map.keys(@changes)) == @changes

    # A change of every live setting closes no WebSocket.
    assert ping(daemon) =~ ~s("type":"pong")
    close(daemon)

    for key <- Config.boot_only() do
      assert RuntimeConfig.put(%{key => Map.fetch!(RuntimeConfig.current(), key)}) ==
               {:error, "#{inspect(key)} is not a live setting"}
    end
  end

  test "invalid settings are rejected and nothing changes" do
    before = RuntimeConfig.get()

    assert RuntimeConfig.put(%{memory_watermark_bytes: 1}) ==
             {:error,
              "memory_watermark_bytes must be 0 or an integer between 268435456 and 68719476736"}

    assert RuntimeConfig.put(%{heal_rate_per_sec: 0}) ==
             {:error, "heal_rate_per_sec must be an integer between 1 and 500"}

    assert RuntimeConfig.put(heal_rate_per_sec: 501) ==
             {:error, "heal_rate_per_sec must be an integer between 1 and 500"}

    assert RuntimeConfig.put(%{heal_delay_ms: -1}) ==
             {:error, "heal_delay_ms must be an integer between 0 and 86400000"}

    assert RuntimeConfig.put(%{heal_enabled: "false"}) ==
             {:error, "heal_enabled must be true or false"}

    assert RuntimeConfig.put(%{tcp_receive_buffer_bytes: nil}) ==
             {:error,
              "tcp_receive_buffer_bytes must be auto or an integer between 4096 and 1048576"}

    assert RuntimeConfig.put(%{cluster_nodes: [:not_a_node]}) ==
             {:error, "cluster_nodes must contain at most 1000 comma-separated name@host peers"}

    assert RuntimeConfig.put(%{port: 4001}) == {:error, ":port is not a live setting"}
    assert RuntimeConfig.put(%{unknown: 1}) == {:error, ":unknown is not a live setting"}

    # Rules between settings apply to the result of the change.
    assert RuntimeConfig.put(%{delivery_timeout_ms: before.transport_send_timeout_ms}) ==
             {:error, "delivery_timeout_ms must be lower than transport_send_timeout_ms"}

    assert RuntimeConfig.put(%{ingress_budget_bytes: 256 * @mebibyte, ingress_weight: 16}) ==
             {:error,
              "ingress_budget_bytes must admit one maximum assembled message at the configured weight"}

    assert RuntimeConfig.put(%{oom_min_available_percent: 20}) ==
             {:error,
              "oom_recovery_available_percent must not be lower than oom_min_available_percent"}

    # One invalid value rejects the whole change.
    assert {:error, _message} = RuntimeConfig.put(%{heal_rate_per_sec: 10, heal_delay_ms: -1})
    assert RuntimeConfig.get() == before
  end

  test "socket options change on open and new sockets without a disconnect" do
    on_exit(&restart_listener/0)
    connections = connection_sockets()
    daemon = open_daemon(server_id("open"))
    [open] = connection_sockets() -- connections

    assert :inet.getopts(open, [:buffer, :send_timeout]) ==
             {:ok, [buffer: 65_536, send_timeout: 35_000]}

    assert {:ok, _settings} =
             RuntimeConfig.put(%{
               tcp_user_buffer_bytes: 32_768,
               tcp_receive_buffer_bytes: 32_768,
               transport_send_timeout_ms: 40_000,
               http_idle_timeout_ms: 20_000
             })

    assert :inet.getopts(open, [:buffer, :send_timeout]) ==
             {:ok, [buffer: 32_768, send_timeout: 40_000]}

    assert receive_buffer(open) in [32_768, 65_536]
    assert ping(daemon) =~ ~s("type":"pong")

    later = open_daemon(server_id("later"))
    [accepted] = connection_sockets() -- [open | connections]

    assert :inet.getopts(accepted, [:buffer, :send_timeout]) ==
             {:ok, [buffer: 32_768, send_timeout: 40_000]}

    assert receive_buffer(accepted) in [32_768, 65_536]
    assert :ranch.get_protocol_options(@listener).idle_timeout == 20_000

    close(daemon)
    close(later)
    await_no_active_websockets()

    # A listener that restarts opens its listen socket with the live settings.
    restart_listener()
    socket_options = :ranch.get_transport_options(@listener).socket_opts
    assert socket_options[:buffer] == 32_768
    assert socket_options[:recbuf] == 32_768
    assert socket_options[:send_timeout] == 40_000
    assert :ranch.get_protocol_options(@listener).idle_timeout == 20_000
  end

  test "a change without a socket option does not touch the open sockets" do
    connections = connection_sockets()
    daemon = open_daemon(server_id("untouched"))
    [open] = connection_sockets() -- connections

    # A value that no setting holds shows whether the node writes the socket.
    :ok = :inet.setopts(open, send_timeout: 12_345)

    assert {:ok, _settings} =
             RuntimeConfig.put(%{connections_per_acceptor: 600, http_idle_timeout_ms: 20_000})

    assert :ranch.get_max_connections(@listener) == 600
    assert :ranch.get_protocol_options(@listener).idle_timeout == 20_000
    assert :inet.getopts(open, [:send_timeout]) == {:ok, [send_timeout: 12_345]}

    assert {:ok, _settings} = RuntimeConfig.put(%{transport_send_timeout_ms: 40_000})
    assert :inet.getopts(open, [:send_timeout]) == {:ok, [send_timeout: 40_000]}

    close(daemon)
    await_no_active_websockets()
  end

  @tag timeout: 30_000
  test "a higher socket ceiling admits WebSockets on a full node and keeps the open ones" do
    assert {:ok, _settings} = RuntimeConfig.put(%{connections_per_acceptor: 1})
    ceiling = Config.max_websockets(RuntimeConfig.current())
    assert ceiling == RuntimeConfig.current().acceptors

    prefix = server_id("full")
    daemons = for index <- 1..ceiling, do: open_daemon("#{prefix}_#{index}")
    refute Operations.ready?()

    assert {:ok, _settings} = RuntimeConfig.put(%{connections_per_acceptor: 2})
    assert :ranch.get_max_connections(@listener) == 2
    assert Operations.ready?()

    extra = open_daemon("#{prefix}_extra")
    assert Capacity.active_connections(@listener) == ceiling + 1
    assert ping(hd(daemons)) =~ ~s("type":"pong")

    Enum.each([extra | daemons], &close/1)
    await_no_active_websockets()
  end

  test "an open socket applies a changed delivery setting to its next message" do
    client = open_client(server_id("attach"), "missing_data_socket")
    assert {:ok, _settings} = RuntimeConfig.put(%{data_attach_timeout_ms: 1_000})

    # No daemon data socket attaches. The default wait is 15 seconds.
    :ok = :gen_tcp.send(client, masked(0x82, "payload"))

    assert {:ok, <<0x88, _length, 1013::16, "Data route unavailable">>} =
             :gen_tcp.recv(client, 0, 3_000)

    :gen_tcp.close(client)
    await_no_active_websockets()
  end

  test "the ingress budget changes for the next message" do
    namespace = {:live_ingress, System.unique_integer([:positive])}
    timeout = RuntimeConfig.current().capacity_mutation_timeout_ms
    message_bytes = 40 * @mebibyte

    assert {:ok, connection} = Capacity.admit_connection(namespace, 1, timeout)
    assert {:ok, _capacity} = Capacity.attach_connection(connection, timeout)

    assert {:ok, _settings} =
             RuntimeConfig.put(%{ingress_budget_bytes: 128 * @mebibyte, ingress_weight: 4})

    assert Capacity.admit_message(message_bytes, timeout) == {:error, :message_exceeds_budget}

    assert {:ok, _settings} = RuntimeConfig.put(%{ingress_weight: 2})
    assert {:ok, message} = Capacity.admit_message(message_bytes, timeout)
    assert Capacity.value(:ingress_reserved_bytes) == 2 * message_bytes

    Capacity.cancel_message(message)
    Capacity.release_connection(connection)
  end

  test "drain and the cluster floor change live" do
    assert Operations.ready?()

    assert {:ok, %{drain: true}} = RuntimeConfig.put(%{drain: true})
    assert PaseoRelay.Drain.draining?()
    refute Operations.ready?()

    :ok = PaseoRelay.Drain.cancel()
    assert RuntimeConfig.get().drain == false
    assert Operations.ready?()

    assert {:ok, _settings} = RuntimeConfig.put(%{minimum_cluster_size: 99})
    refute Operations.ready?()
    assert upgrade(server_id("below_floor"), "server") =~ "HTTP/1.1 503"
  end

  test "Capacity settings show in metrics and survive a Capacity restart" do
    settings = %{memory_watermark_bytes: @watermark, ingress_weight: 2}
    assert {:ok, %{memory_watermark_bytes: @watermark}} = RuntimeConfig.put(settings)
    assert metric("memory_watermark_bytes") == @watermark

    restart(Capacity)
    assert metric("memory_watermark_bytes") == @watermark
    assert Map.take(RuntimeConfig.get(), Map.keys(settings)) == settings
  end

  test "a change Capacity cannot apply is rejected and nothing changes" do
    assert {:ok, _settings} = RuntimeConfig.put(%{capacity_mutation_timeout_ms: 200})
    before = RuntimeConfig.get()
    capacity = Process.whereis(Capacity)
    :ok = :sys.suspend(capacity)

    assert RuntimeConfig.put(%{memory_watermark_bytes: @watermark, heal_rate_per_sec: 9}) ==
             {:error, "Capacity is unavailable; nothing changed"}

    # Capacity fails closed and restarts with the previous settings.
    await_replacement(Capacity, capacity, deadline())
    assert RuntimeConfig.get() == before
    assert metric("memory_watermark_bytes") == before.memory_watermark_bytes
  end

  test "a setting that Capacity does not hold changes while Capacity is stalled" do
    capacity = Process.whereis(Capacity)
    :ok = :sys.suspend(capacity)

    try do
      assert PaseoRelay.Drain.begin() == :ok
      assert PaseoRelay.Drain.draining?()
      assert {:ok, %{heal_rate_per_sec: 9}} = RuntimeConfig.put(%{heal_rate_per_sec: 9})
    after
      :sys.resume(capacity)
    end

    assert Process.whereis(Capacity) == capacity
  end

  defp server_id(name), do: "srv_runtime_#{name}_#{System.unique_integer([:positive])}"

  defp metric(name) do
    {200, _content_type, body} = Operations.response("/metrics")
    [_, value] = Regex.run(~r/^paseo_relay_#{name} (\d+)$/m, body)
    String.to_integer(value)
  end

  defp deadline, do: System.monotonic_time(:millisecond) + 5_000

  defp restart(name) do
    process = Process.whereis(name)
    reference = Process.monitor(process)
    Process.exit(process, :kill)
    assert_receive {:DOWN, ^reference, :process, ^process, :killed}
    await_replacement(name, process, deadline())
  end

  # Stops the listener and starts it again, as its supervisor does after a
  # failure. The VM closes the old listen socket after the listener stops, so
  # the port can still be in use for the first attempts.
  defp restart_listener do
    :ok = Supervisor.terminate_child(PaseoRelay.RuntimeSupervisor, @listener)

    await(fn ->
      match?({:ok, _listener}, Supervisor.restart_child(PaseoRelay.RuntimeSupervisor, @listener))
    end)
  end

  defp await_replacement(name, previous, deadline) do
    case Process.whereis(name) do
      replacement when is_pid(replacement) and replacement != previous ->
        # The listener restarts after Capacity, so wait until it serves again.
        await_listener(deadline)

      _missing ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("#{name} did not restart")
        Process.sleep(10)
        await_replacement(name, previous, deadline)
    end
  end

  defp await_listener(deadline) do
    case :ranch.get_port(@listener) do
      port when is_integer(port) ->
        :ok

      _missing ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("listener did not restart")
        Process.sleep(10)
        await_listener(deadline)
    end
  catch
    _kind, _reason ->
      Process.sleep(10)
      await_listener(deadline)
  end

  # The server-side TCP sockets of the production listener.
  defp connection_sockets do
    for connection <- :ranch.procs(@listener, :connections),
        {:links, links} <- [Process.info(connection, :links)],
        socket <- links,
        is_port(socket),
        do: socket
  end

  # Linux reports double the requested SO_RCVBUF.
  defp receive_buffer(socket) do
    {:ok, [recbuf: bytes]} = :inet.getopts(socket, [:recbuf])
    bytes
  end

  defp open_daemon(server_id) do
    {socket, response} = request_upgrade(server_id, "server", "")
    assert "HTTP/1.1 101" <> _rest = response

    # The Owner sends its initial sync notification, with the response or after it.
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, ""] -> assert {:ok, <<0x81, _sync::binary>>} = :gen_tcp.recv(socket, 0, 2_000)
      [_headers, <<0x81, _sync::binary>>] -> :ok
    end

    socket
  end

  defp open_client(server_id, connection_id) do
    {socket, response} = request_upgrade(server_id, "client", "&connectionId=#{connection_id}")
    assert "HTTP/1.1 101" <> _rest = response
    socket
  end

  defp upgrade(server_id, role) do
    {socket, response} = request_upgrade(server_id, role, "")
    :gen_tcp.close(socket)
    response
  end

  defp request_upgrade(server_id, role, query) do
    port = PaseoRelay.Listener.port(@listener)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        socket,
        "GET /ws?serverId=#{server_id}&role=#{role}&v=2#{query} HTTP/1.1\r\n" <>
          "Host: relay.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" <>
          "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
      )

    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    {socket, response}
  end

  defp ping(daemon) do
    :ok = :gen_tcp.send(daemon, masked(0x81, ~s({"type":"ping"})))
    assert {:ok, <<0x81, _length, pong::binary>>} = :gen_tcp.recv(daemon, 0, 2_000)
    pong
  end

  defp masked(first_byte, payload) do
    mask = :crypto.strong_rand_bytes(4)
    <<first_byte, 0x80 + byte_size(payload), mask::binary, xor(payload, mask)::binary>>
  end

  defp xor(payload, <<a, b, c, d>>) do
    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, index} -> Bitwise.bxor(byte, elem({a, b, c, d}, rem(index, 4))) end)
    |> :erlang.list_to_binary()
  end

  defp close(socket) do
    :ok = :gen_tcp.send(socket, <<0x88, 0x80, :crypto.strong_rand_bytes(4)::binary>>)
    _ = :gen_tcp.recv(socket, 0, 2_000)
    :gen_tcp.close(socket)
  end

  defp await_no_active_websockets,
    do: await(fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end)

  defp await(condition, deadline \\ nil) do
    deadline = deadline || deadline()

    cond do
      condition.() -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("condition was not met in time")
      true -> Process.sleep(10) && await(condition, deadline)
    end
  end
end
