defmodule PaseoRelay.BackpressureTest do
  use ExUnit.Case, async: false
  import Bitwise

  @frame_bytes 4 * 1024 * 1024
  @pressure_frame_bytes 8 * 1024 * 1024
  @maximum_frame_wire_bytes 32 * 1024 * 1024
  @maximum_client_frame_header_bytes 14
  @maximum_client_frame_payload_bytes @maximum_frame_wire_bytes -
                                        @maximum_client_frame_header_bytes
  @maximum_message_payload_bytes PaseoRelay.Protocol.maximum_message_payload_bytes()
  @capacity_mutation_timeout_ms PaseoRelay.Config.defaults().capacity_mutation_timeout_ms
  @transport_send_timeout_ms PaseoRelay.Config.defaults().transport_send_timeout_ms
  @maximum_control_payload_bytes PaseoRelay.Protocol.maximum_control_payload_bytes()
  @control_setup_timeout_ms 3_000

  setup do
    {:ok, resources} = Agent.start(fn -> [] end)
    Process.put(:relay_test_resources, resources)

    baseline = %{
      active_websockets: 0,
      backpressured_sources: 0,
      inflight_delivery_bytes: 0,
      ingress_reserved_bytes: 0
    }

    await_transient_gauges(baseline)

    on_exit(fn ->
      tracked = Agent.get(resources, & &1)
      {listeners, connections} = Enum.split_with(tracked, &match?({:listener, _}, &1))

      {raw_sockets, managed_clients} =
        Enum.split_with(connections, &match?({:socket, _}, &1))

      Enum.each(raw_sockets, &stop_resource/1)
      Enum.each(managed_clients, &stop_resource/1)
      await_transient_gauges(baseline)
      Enum.each(listeners, &stop_resource/1)
      if Process.alive?(resources), do: Agent.stop(resources)
    end)

    :ok
  end

  defmodule RelayClient do
    use WebSockex

    def start_link(url, owner), do: WebSockex.start_link(url, __MODULE__, owner)

    def handle_connect(_connection, owner) do
      send(owner, {:relay_open, self()})
      {:ok, owner}
    end

    def handle_frame({kind, payload}, owner) do
      send(owner, {:relay_frame, self(), kind, payload})
      {:ok, owner}
    end

    def handle_disconnect(%{reason: reason}, owner) do
      send(owner, {:relay_closed, self(), reason})
      {:ok, owner}
    end

    def handle_cast(:close, owner), do: {:close, owner}
  end

  defmodule DigestClient do
    use WebSockex

    def start_link(url, owner), do: WebSockex.start_link(url, __MODULE__, owner)

    def handle_connect(_connection, owner) do
      send(owner, {:digest_open, self()})
      {:ok, owner}
    end

    def handle_frame({kind, payload}, owner) do
      send(
        owner,
        {:digest_frame, self(), kind, byte_size(payload), :crypto.hash(:sha256, payload)}
      )

      {:ok, owner}
    end

    def handle_cast(:close, owner), do: {:close, owner}
  end

  test "a client frame waits without buffering until daemon data attaches" do
    port = start_relay()
    source = raw_connect(port, "/ws?serverId=attach-#{port}&role=client&v=2&connectionId=c1")

    :ok = send_frame(source, :text, "first")
    await_metric(:backpressured_sources, &(&1 == 1))
    :ok = send_frame(source, :binary, "second")

    {:ok, destination} =
      connect(
        "ws://127.0.0.1:#{port}/ws?serverId=attach-#{port}&role=server&v=2&connectionId=c1",
        self()
      )

    assert_receive {:relay_open, ^destination}
    assert_receive {:relay_frame, ^destination, :text, "first"}
    assert_receive {:relay_frame, ^destination, :binary, "second"}
    await_metric(:backpressured_sources, &(&1 == 0))
    stop_resource({:client, destination})
    close_raw(source)
    await_metric(:active_websockets, &(&1 == 0))
  end

  test "an alive but stalled Owner cannot retain a source past its delivery deadline" do
    port = start_relay(delivery_timeout: 200, data_attach_timeout: 1_000)
    server_id = "stalled-owner-#{port}"
    source = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")

    destination =
      raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")

    assert :ok = send_frame(source, :text, "owner-ready")
    assert {:text, "owner-ready"} = recv_server_frame(destination)
    owner = PaseoRelay.Ownership.owner_pid(server_id)
    owner_ref = Process.monitor(owner)
    :ok = :sys.suspend(owner)

    on_exit(fn ->
      if Process.alive?(owner), do: :sys.resume(owner)
    end)

    assert :ok = send_frame(source, :text, "bounded-owner-lookup")
    assert {:close, source_code, source_reason} = recv_until_close(source)

    assert {source_code, source_reason} in [
             {1012, "Session owner moved"},
             {1013, "Delivery unavailable"}
           ]

    assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}, 1_000
    await_unowned(server_id)
    await_reserved(&(&1 == 0))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    close_raw(source)
    assert {:close, 1012, "Session owner moved"} = recv_until_close(destination)
    close_raw(destination)
    await_metric(:active_websockets, &(&1 == 0))
  end

  @tag timeout: 20_000
  test "a passive destination bounds relay payloads and stalls the source TCP sender" do
    port = start_relay(delivery_timeout: 500, send_timeout: 1_000)
    active_baseline = PaseoRelay.Metrics.value(:active_websockets)
    slow_baseline = PaseoRelay.Metrics.value(:slow_consumer_disconnects)
    destination = raw_connect(port, "/ws?serverId=pressure-#{port}&role=server")
    source = raw_connect(port, "/ws?serverId=pressure-#{port}&role=client")
    payload = :binary.copy(<<42>>, @frame_bytes)

    sender =
      Task.async(fn ->
        Enum.reduce_while(1..32, :ok, fn _, _result ->
          case send_frame(source, :binary, payload) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end)

    await_metric(:backpressured_sources, &(&1 >= 1))
    assert PaseoRelay.Metrics.value(:inflight_delivery_bytes) <= @frame_bytes
    assert nil == Task.yield(sender, 250)

    await_metric(:slow_consumer_disconnects, &(&1 == slow_baseline + 1))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    close_raw(source)
    close_raw(destination)
    _ = Task.await(sender, 5_000)
    await_metric(:active_websockets, &(&1 == active_baseline))
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 45_000
  test "suspended source reads leave outbound Writer delivery live" do
    port = start_relay(delivery_timeout: 15_000, send_timeout: 20_000, send_buffer: 1024)
    server_id = "full-duplex-#{port}"

    slow = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")

    {:ok, healthy} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^healthy}

    daemon = raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")
    payload = :binary.copy(<<0x2A>>, @pressure_frame_bytes)

    sender =
      Task.async(fn ->
        Enum.reduce_while(1..32, :ok, fn _, _result ->
          case send_frame(daemon, :binary, payload) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end)

    await_stable_metric(:backpressured_sources, &(&1 == 1), 250)
    assert_receive {:relay_frame, ^healthy, :binary, ^payload}, 30_000
    assert :ok = WebSockex.send_frame(healthy, {:text, "reverse-during-pressure"})
    assert {:text, "reverse-during-pressure"} = recv_server_frame(daemon)
    await_stable_metric(:backpressured_sources, &(&1 == 1), 250)

    close_raw(slow)
    close_raw(daemon)
    _ = Task.await(sender, 10_000)
    stop_resource({:client, healthy})
    await_metric(:backpressured_sources, &(&1 == 0))
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 90_000
  test "completed messages cannot exceed the strict node byte budget" do
    port = start_relay(data_attach_timeout: 60_000)
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    payload = :binary.copy(<<0x5A>>, @maximum_message_payload_bytes)

    sockets =
      Enum.map(1..5, fn index ->
        raw_connect(
          port,
          "/ws?serverId=budget-#{port}-#{index}&role=client&v=2&connectionId=missing"
        )
      end)

    await_metric(:active_websockets, &(&1 == baseline + 5))

    Enum.each(Enum.take(sockets, 4), fn socket ->
      assert :ok = send_frame(socket, :binary, payload)
    end)

    limit = @maximum_message_payload_bytes * 4 * 4
    await_reserved(&(&1 == limit))
    assert PaseoRelay.Capacity.value(:ingress_reserved_bytes) == limit

    rejected = List.last(sockets)
    assert :ok = send_frame(rejected, :binary, payload)
    assert {:close, 1013, "Relay ingress capacity"} = recv_server_frame(rejected)
    assert PaseoRelay.Capacity.value(:ingress_reserved_bytes) == limit

    digest = :crypto.hash(:sha256, payload)

    destinations =
      Enum.map(1..4, fn index ->
        digest_connect(v2_url(port, "budget-#{port}-#{index}", "server", "missing"))
      end)

    Enum.each(destinations, fn destination ->
      assert_receive {:digest_frame, ^destination, :binary, @maximum_message_payload_bytes,
                      ^digest},
                     30_000
    end)

    await_reserved(&(&1 == 0))
    Enum.each(Enum.take(sockets, 4), &close_raw/1)
    Enum.each(destinations, &stop_resource({:client, &1}))
    await_metric(:active_websockets, &(&1 == baseline))
  end

  test "pipelined frames retain order and reserve one active delivery" do
    port = start_relay()
    server_id = "pipelined-#{port}"
    payloads = Enum.map(1..3, &<<&1, :binary.copy(<<&1>>, 1024 * 1024 - 1)::binary>>)

    source =
      raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")

    baseline = PaseoRelay.Capacity.value(:ingress_reserved_bytes)
    assert :ok = send_frames(source, Enum.map(payloads, &{:binary, &1}))

    expected = baseline + byte_size(hd(payloads)) * 4
    await_reserved(&(&1 == expected))
    assert PaseoRelay.Capacity.value(:ingress_reserved_bytes) == expected

    {:ok, destination} = connect(v2_url(port, server_id, "server", "shared"))
    assert_receive {:relay_open, ^destination}

    Enum.each(payloads, fn payload ->
      assert_receive {:relay_frame, ^destination, :binary, ^payload}, 5_000
      assert PaseoRelay.Capacity.value(:ingress_reserved_bytes) <= expected
    end)

    await_reserved(&(&1 == baseline))
    close_raw(source)
    await_metric(:active_websockets, &(&1 == 0))
  end

  @tag timeout: 30_000
  test "a maximum legal unfragmented payload survives the heap fuse" do
    port = start_relay()
    server_id = "maximum-frame-#{port}"

    destination =
      raw_connect(port, v2_path(server_id, "server", "shared"), receive_buffer: 4 * 1024 * 1024)

    source = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    payload = :binary.copy(<<0xA5>>, @maximum_client_frame_payload_bytes)
    digest = :crypto.hash(:sha256, payload)
    receiver = Task.async(fn -> recv_server_frame(destination) end)

    assert :ok = send_frame(source, :binary, payload)

    assert {:binary, delivered} = Task.await(receiver, 30_000)
    assert byte_size(delivered) == @maximum_client_frame_payload_bytes
    assert :crypto.hash(:sha256, delivered) == digest

    await_reserved(&(&1 == 0))
    close_raw(source)
    close_raw(destination)
    await_metric(:active_websockets, &(&1 == 0))
  end

  @tag timeout: 45_000
  test "a fragmented maximum-size message permits an interleaved control frame" do
    port = start_relay()
    server_id = "maximum-fragmented-#{port}"
    destination = digest_connect(v2_url(port, server_id, "server", "shared"))
    source = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    await_metric(:active_websockets, &(&1 == 2))
    half = :binary.copy(<<0x3C>>, div(@maximum_message_payload_bytes, 2))
    digest = :crypto.hash(:sha256, half <> half)

    assert :ok = send_raw_frame(source, 0x2, half, false)
    assert :ok = send_raw_frame(source, 0x9, "still-alive", true)
    assert {:pong, "still-alive"} = recv_server_frame(source)
    assert :ok = send_raw_frame(source, 0x0, half, true)

    assert_receive {:digest_frame, ^destination, :binary, @maximum_message_payload_bytes,
                    ^digest},
                   @transport_send_timeout_ms

    await_reserved(&(&1 == 0))
    close_raw(source)
    await_metric(:active_websockets, &(&1 == 0))
  end

  test "incomplete fragments remain outside relay admission at the public Cowboy boundary" do
    port = start_relay()
    source = raw_connect(port, "/ws?serverId=incomplete-fragment-#{port}&role=server")
    baseline = PaseoRelay.Capacity.value(:ingress_reserved_bytes)
    fragment = :binary.copy(<<0x7A>>, 1024 * 1024)

    assert :ok = send_raw_frame(source, 0x2, fragment, false)
    assert :ok = send_raw_frame(source, 0x9, "before-wait", true)
    assert {:pong, "before-wait"} = recv_server_frame(source)
    Process.sleep(250)
    assert PaseoRelay.Capacity.value(:ingress_reserved_bytes) == baseline
    assert :ok = send_raw_frame(source, 0x9, "after-wait", true)
    assert {:pong, "after-wait"} = recv_server_frame(source)

    close_raw_websocket(source)
    await_reserved(&(&1 == baseline))
  end

  test "Cowboy rejects a frame one byte over the 32 MiB wire ceiling with 1009" do
    port = start_relay()
    source = raw_connect(port, "/ws?serverId=oversize-#{port}&role=server")

    assert :ok = send_frame_header(source, :binary, @maximum_client_frame_payload_bytes + 1)
    assert {:close, 1009, _reason} = recv_server_frame(source)
    await_reserved(&(&1 == 0))
  end

  test "concurrent producers retain per-source FIFO order through one writer" do
    port = start_relay()
    server_id = "producers-#{port}"

    {:ok, destination} = connect(v2_url(port, server_id, "server", "shared"))
    assert_receive {:relay_open, ^destination}

    sources =
      Enum.map(1..5, fn _ ->
        {:ok, source} = connect(v2_url(port, server_id, "client", "shared"))
        assert_receive {:relay_open, ^source}
        source
      end)

    sources
    |> Enum.with_index(1)
    |> Task.async_stream(fn {source, source_id} ->
      Enum.each(1..20, fn sequence ->
        :ok = WebSockex.send_frame(source, {:text, "#{source_id}:#{sequence}"})
      end)
    end)
    |> Stream.run()

    received =
      Enum.map(1..100, fn _ ->
        assert_receive {:relay_frame, ^destination, :text, payload}, 5_000
        payload
      end)

    assert received
           |> Enum.map(&String.split(&1, ":"))
           |> Enum.group_by(&hd/1, &(List.last(&1) |> String.to_integer()))
           |> Map.values()
           |> Enum.all?(&(&1 == Enum.to_list(1..20)))

    Enum.each(sources, &stop_resource({:client, &1}))
  end

  test "control notifications retain the forwarded metric contract" do
    port = start_relay()
    server_id = "control-metrics-#{port}"
    active_baseline = PaseoRelay.Metrics.value(:active_websockets)
    frames_baseline = PaseoRelay.Metrics.value(:frames_forwarded)
    bytes_baseline = PaseoRelay.Metrics.value(:bytes_forwarded)

    control = raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2")
    assert {:text, sync} = recv_server_frame(control)

    client =
      raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")

    assert {:text, connected} = recv_server_frame(control)

    assert Jason.decode!(sync) == %{"connectionIds" => [], "type" => "sync"}
    assert Jason.decode!(connected) == %{"connectionId" => "shared", "type" => "connected"}
    await_metric(:frames_forwarded, &(&1 == frames_baseline + 2))

    expected_bytes = bytes_baseline + byte_size(sync) + byte_size(connected)
    await_metric(:bytes_forwarded, &(&1 == expected_bytes))

    close_raw_websocket(control)
    close_raw_websocket(client)
    await_metric(:active_websockets, &(&1 == active_baseline))
  end

  @tag timeout: 15_000
  test "an accepted control notification cannot expire silently in the Writer queue" do
    existing_writers = writer_processes()

    port =
      start_relay(
        delivery_timeout: 1_000,
        send_timeout: @control_setup_timeout_ms,
        control_queue: 1024 * 1024
      )

    server_id = "control-deadline-#{port}"
    control = raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2")
    assert {:text, _sync} = recv_server_frame(control, @control_setup_timeout_ms)
    writer = await_new_writer(existing_writers)
    [control_process] = ranch_connections(port)
    :erlang.suspend_process(control_process)

    on_exit(fn -> resume_process(control_process) end)

    assert :ok = PaseoRelay.Delivery.Writer.control(writer, ~s({"type":"connected","id":"first"}))

    assert :ok =
             PaseoRelay.Delivery.Writer.control(writer, ~s({"type":"connected","id":"second"}))

    :ok = :sys.suspend(writer)

    on_exit(fn ->
      if Process.alive?(writer), do: :sys.resume(writer)
    end)

    resume_process(control_process)

    receive do
    after
      1_100 -> :ok
    end

    :ok = :sys.resume(writer)
    close = Task.async(fn -> recv_until_close(control) end)
    assert {:close, 1013, "Slow consumer"} = Task.await(close, 2_000)
    close_raw(control)
    await_metric(:active_websockets, &(&1 == 0))
  end

  @tag timeout: 60_000
  test "an unread control socket is shed through its bounded Writer" do
    port =
      start_relay(
        control_queue: 64,
        delivery_timeout: 500,
        send_timeout: 1_000,
        send_buffer: 1024
      )

    server_id = "slow-control-#{port}"
    active_baseline = PaseoRelay.Metrics.value(:active_websockets)
    control = raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2")

    clients =
      Enum.map(1..1_000, fn index ->
        connection_id = index |> Integer.to_string() |> String.pad_trailing(256, "x")

        raw_connect(
          port,
          "/ws?serverId=#{server_id}&role=client&v=2&connectionId=#{connection_id}"
        )
      end)

    resources = Process.get(:relay_test_resources)

    burst_clients =
      1..100
      |> Task.async_stream(
        fn index ->
          Process.put(:relay_test_resources, resources)

          raw_connect(
            port,
            "/ws?serverId=#{server_id}&role=client&v=2&connectionId=#{String.pad_trailing("z#{index}", 256, "z")}"
          )
        end,
        max_concurrency: 100,
        timeout: 10_000,
        ordered: false
      )
      |> Enum.map(fn {:ok, socket} -> socket end)

    assert {:close, 1013, "Slow consumer"} = recv_until_close(control)
    Enum.each(burst_clients ++ clients, &close_raw/1)
    close_raw(control)
    await_metric(:active_websockets, &(&1 == active_baseline))
  end

  test "a Writer crash closes its real Cowboy websocket" do
    existing = writer_processes()
    port = start_relay()
    {:ok, socket} = connect("ws://127.0.0.1:#{port}/ws?serverId=writer-death-#{port}&role=server")
    assert_receive {:relay_open, ^socket}

    writer = await_new_writer(existing)
    Process.exit(writer, :kill)

    assert_receive {:relay_closed, ^socket, {:remote, 1013, "Delivery unavailable"}}, 2_000
  end

  test "a Writer rejects successors when its active source dies behind a write barrier" do
    {:ok, writer} = PaseoRelay.Delivery.Writer.start(self(), 5_000, 1024 * 1024)
    writer_ref = Process.monitor(writer)

    first =
      Task.async(fn ->
        with {:ok, token} <-
               PaseoRelay.Delivery.Writer.reserve(
                 writer,
                 5,
                 PaseoRelay.Delivery.Deadline.after_ms(5_000)
               ) do
          PaseoRelay.Delivery.Writer.write(
            writer,
            token,
            :binary,
            "first",
            PaseoRelay.Delivery.Deadline.after_ms(5_000)
          )
        end
      end)

    Process.unlink(first.pid)
    assert_receive {:relay_frame, ^writer, first_reference, :binary, "first"}
    assert_receive {:relay_write_barrier, ^writer, ^first_reference}

    second =
      Task.async(fn ->
        with {:ok, token} <-
               PaseoRelay.Delivery.Writer.reserve(
                 writer,
                 6,
                 PaseoRelay.Delivery.Deadline.after_ms(5_000)
               ) do
          PaseoRelay.Delivery.Writer.write(
            writer,
            token,
            :binary,
            "second",
            PaseoRelay.Delivery.Deadline.after_ms(5_000)
          )
        end
      end)

    assert nil == Task.yield(second, 100)
    Process.exit(first.pid, :kill)

    assert_receive {:relay_close, 1013, "Delivery unavailable"}, 1_000
    assert_receive {:DOWN, ^writer_ref, :process, ^writer, :normal}, 1_000
    refute_receive {:relay_frame, ^writer, _reference, :binary, "second"}
    assert {:error, :source_closed} = Task.await(second, 1_000)
  end

  test "an oversized v2 control message is rejected before JSON parsing" do
    port = start_relay()
    control = raw_connect(port, "/ws?serverId=control-limit-#{port}&role=server&v=2")
    payload = :binary.copy(<<0x20>>, @maximum_control_payload_bytes + 1)

    assert :ok = send_frame(control, :text, payload)
    assert {:close, 1009, _reason} = recv_until_close(control)
  end

  test "a heap-fuse kill during delivery reconciles every capacity gauge" do
    low_heap_port =
      start_relay(
        websocket_heap_words: 3_145_728,
        delivery_timeout: 15_000,
        send_timeout: 20_000,
        send_buffer: 1024
      )

    normal_port = start_relay(delivery_timeout: 15_000, send_timeout: 20_000, send_buffer: 1024)
    baseline = transient_gauges()
    server_id = "heap-delivery-#{low_heap_port}"
    slow = raw_connect(normal_port, v2_path(server_id, "client", "shared"))

    {:ok, healthy} = connect(v2_url(normal_port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^healthy}

    source = raw_connect(low_heap_port, v2_path(server_id, "server", "shared"))
    payload = :binary.copy(<<0x5A>>, 8 * 1024 * 1024)

    sender =
      Task.async(fn ->
        Enum.reduce_while(1..32, :ok, fn _, _result ->
          case send_frame(source, :binary, payload) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end)

    await_metric(:backpressured_sources, &(&1 == 1))
    assert_receive {:relay_frame, ^healthy, :binary, ^payload}, 30_000

    assert :ok =
             WebSockex.send_frame(
               healthy,
               {:binary, :binary.copy(<<0x6B>>, @maximum_client_frame_payload_bytes)}
             )

    assert :ok = await_transport_close(source, 30_000)
    await_metric(:backpressured_sources, &(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    _ = Task.await(sender, 5_000)

    close_raw(slow)
    stop_resource({:client, healthy})
    await_transient_gauges(baseline)
  end

  test "the node watermark explicitly closes the oldest blocked source" do
    on_exit(fn -> reconfigure_pressure(0) end)
    port = start_relay()
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    {:ok, source} = connect(v2_url(port, "watermark-#{port}", "client", "missing"))
    assert_receive {:relay_open, ^source}
    await_metric(:active_websockets, &(&1 == baseline + 1))
    :ok = WebSockex.send_frame(source, {:binary, "blocked"})
    await_metric(:backpressured_sources, &(&1 == 1))
    reconfigure_pressure(1)
    :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)

    assert_receive {:relay_closed, ^source, {:remote, 1013, "Relay memory pressure"}}, 2_000
    await_metric(:active_websockets, &(&1 == baseline))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 20_000
  test "pressure cancellation after a queued write fails the destination closed" do
    on_exit(fn -> reconfigure_pressure(0) end)
    port = start_relay(delivery_timeout: 5_000, send_timeout: 6_000, send_buffer: 1024)
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    server_id = "cancelled-write-#{port}"

    destination =
      raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")

    source =
      raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")

    payload = :binary.copy(<<0x51>>, @frame_bytes)

    sender =
      Task.async(fn ->
        Enum.reduce_while(1..32, :ok, fn _, _result ->
          case send_frame(source, :binary, payload) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end)

    await_stable_metric(:backpressured_sources, &(&1 == 1), 250)

    padding = :binary.copy(<<0x52>>, 40 * 1024 * 1024)
    :erlang.garbage_collect(self())
    reconfigure_pressure(:erlang.memory(:total) - 8 * 1024 * 1024)
    assert :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)
    assert byte_size(padding) == 40 * 1024 * 1024
    reconfigure_pressure(0)

    assert recv_until_close(source) in [
             {:close, 1013, "Relay memory pressure"},
             {:transport_error, :closed}
           ]

    assert {:close, code, reason} = recv_until_close(destination)

    assert {code, reason} in [
             {1001, "Client disconnected"},
             {1013, "Delivery unavailable"}
           ]

    _ = Task.await(sender, 5_000)
    close_raw(source)
    close_raw(destination)
    await_metric(:active_websockets, &(&1 == baseline))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_reserved(&(&1 == 0))
  end

  test "the node watermark closes an incomplete fragmented-message source" do
    on_exit(fn -> reconfigure_pressure(0) end)
    port = start_relay()
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    reserved = PaseoRelay.Capacity.value(:ingress_reserved_bytes)
    source = raw_connect(port, "/ws?serverId=fragment-watermark-#{port}&role=server")
    fragment = :binary.copy(<<0x6B>>, 1024 * 1024)

    await_metric(:active_websockets, &(&1 == baseline + 1))
    assert :ok = send_raw_frame(source, 0x2, fragment, false)
    assert :ok = send_raw_frame(source, 0x9, "fragment-retained", true)
    assert {:pong, "fragment-retained"} = recv_server_frame(source)
    reconfigure_pressure(1)
    assert :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)

    assert {:close, 1013, "Relay memory pressure"} = recv_until_close(source)
    await_metric(:active_websockets, &(&1 == baseline))
    await_reserved(&(&1 == reserved))
  end

  @tag timeout: 30_000
  test "a pressure episode pauses admission and measures relief from fragment sources" do
    on_exit(fn -> reconfigure_pressure(0) end)
    port = start_relay()

    idle =
      Enum.map(1..3, fn index ->
        raw_connect(port, "/ws?serverId=pressure-idle-#{port}-#{index}&role=server")
      end)

    fragment_connections =
      Enum.map(1..5, fn index ->
        existing_connections = MapSet.new(ranch_connections(port))

        source =
          raw_connect(port, "/ws?serverId=pressure-fragment-#{port}-#{index}&role=server")

        assert :ok =
                 send_raw_frame(source, 0x2, :binary.copy(<<index>>, 8 * 1024 * 1024), false)

        pong = "retained-#{index}"
        assert :ok = send_raw_frame(source, 0x9, pong, true)
        assert {:pong, ^pong} = recv_server_frame(source)

        [connection_process] =
          port
          |> ranch_connections()
          |> MapSet.new()
          |> MapSet.difference(existing_connections)
          |> MapSet.to_list()

        {source, connection_process}
      end)

    fragments = Enum.map(fragment_connections, &elem(&1, 0))
    {_newest_fragment, newest_process} = List.last(fragment_connections)
    :erlang.suspend_process(newest_process)
    on_exit(fn -> resume_process(newest_process) end)

    :erlang.garbage_collect(self())
    watermark = :erlang.memory(:total) - 16 * 1024 * 1024
    assert watermark > PaseoRelay.Protocol.maximum_message_payload_bytes()
    reconfigure_pressure(watermark)
    assert :ok = PaseoRelay.Capacity.check_now(@capacity_mutation_timeout_ms)

    {:ok, rejected, response} =
      upgrade_once(port, "/ws?serverId=pressure-paused-#{port}&role=server")

    track({:socket, rejected})
    assert response =~ "HTTP/1.1 503 Service Unavailable"
    assert response =~ "Relay memory pressure"
    assert http_get(port, "/ready") =~ "HTTP/1.1 503 Service Unavailable"
    close_raw(rejected)

    resume_process(newest_process)
    assert {:close, 1013, "Relay memory pressure"} = recv_until_close(List.last(fragments))

    replacement =
      reconnect_after_pressure(port, "/ws?serverId=pressure-resumed-#{port}&role=server")

    assert http_get(port, "/ready") =~ "HTTP/1.1 200 OK"

    :ok = send_raw_frame(hd(idle), 0x9, "idle-survived", true)
    assert {:pong, "idle-survived"} = recv_server_frame(hd(idle))

    close_raw_websocket(replacement)
    Enum.each(idle, &close_raw_websocket/1)
    Enum.each(fragments, &close_raw/1)
    await_metric(:active_websockets, &(&1 == 0))
  end

  @tag timeout: 15_000
  test "a timed-out completed frame invalidates every socket in its Capacity epoch" do
    port = PaseoRelay.Listener.port(PaseoRelay.Listener)
    existing_connections = MapSet.new(:ranch.procs(PaseoRelay.Listener, :connections))

    source =
      raw_connect(
        port,
        "/ws?serverId=stalled-message-#{port}&role=client&v=2&connectionId=missing"
      )

    old_connection = await_ranch_connection(existing_connections)
    connection_monitor = Process.monitor(old_connection)

    idle = raw_connect(port, "/ws?serverId=stalled-message-idle-#{port}&role=server&v=2")
    idle_connection = await_ranch_connection(MapSet.put(existing_connections, old_connection))
    idle_monitor = Process.monitor(idle_connection)
    await_metric(:active_websockets, &(&1 == 2))

    capacity = Process.whereis(PaseoRelay.Capacity)
    listener = runtime_child(PaseoRelay.Listener)
    capacity_monitor = Process.monitor(capacity)
    listener_monitor = Process.monitor(listener)
    :ok = :sys.suspend(capacity)

    on_exit(fn ->
      if Process.alive?(capacity), do: :sys.resume(capacity)
    end)

    assert :ok = send_frame(source, :text, "completed-before-capacity-decision")
    assert {:error, :timeout} = :gen_tcp.recv(source, 0, 4_500)
    assert_receive {:DOWN, ^capacity_monitor, :process, ^capacity, :killed}, 2_000
    assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :shutdown}, 2_000
    assert_receive {:DOWN, ^connection_monitor, :process, ^old_connection, _source_reason}, 2_000
    assert_receive {:DOWN, ^idle_monitor, :process, ^idle_connection, _idle_reason}, 2_000

    assert recv_until_close(source) in [
             {:close, 1013, "Relay capacity unavailable"},
             {:close, 1013, "Relay ingress capacity"},
             {:transport_error, :closed}
           ]

    replacement_capacity = await_replacement(PaseoRelay.Capacity, capacity)
    replacement_listener = await_runtime_child_replacement(PaseoRelay.Listener, listener)
    assert is_pid(replacement_capacity)
    assert replacement_listener != listener

    await_transient_gauges(%{
      active_websockets: 0,
      backpressured_sources: 0,
      inflight_delivery_bytes: 0,
      ingress_reserved_bytes: 0
    })

    replacement =
      raw_connect(port, "/ws?serverId=stalled-message-replacement-#{port}&role=server&v=2")

    assert :ok = send_frame(replacement, :text, Jason.encode!(%{type: "ping"}))
    assert %{"type" => "pong"} = recv_control_type(replacement, "pong")

    close_raw(source)
    close_raw(idle)
    close_raw_websocket(replacement)
    await_metric(:active_websockets, &(&1 == 0))
    await_reserved(&(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    await_metric(:backpressured_sources, &(&1 == 0))
  end

  test "a capacity restart drains retained payloads before production admission reopens" do
    port = PaseoRelay.Listener.port(PaseoRelay.Listener)
    baseline = PaseoRelay.Metrics.value(:active_websockets)

    socket =
      raw_connect(port, "/ws?serverId=budget-death-#{port}&role=client&v=2&connectionId=missing")

    old_connection = await_ranch_connection(MapSet.new())
    connection_monitor = Process.monitor(old_connection)
    await_metric(:active_websockets, &(&1 == baseline + 1))
    assert :ok = send_frame(socket, :binary, "retained")
    await_metric(:backpressured_sources, &(&1 == 1))

    old_capacity = Process.whereis(PaseoRelay.Capacity)
    old_listener = runtime_child(PaseoRelay.Listener)
    listener_monitor = Process.monitor(old_listener)
    parent = self()

    reconnect =
      Task.async(fn ->
        receive do
          :reconnect ->
            send(parent, :reconnect_started)

            result =
              reconnect_until_up(
                port,
                v2_path("budget-after-#{port}", "server", ""),
                old_connection
              )

            {replacement, _old_alive?} = result
            :ok = :gen_tcp.controlling_process(replacement, parent)
            result
        end
      end)

    Process.exit(old_capacity, :kill)
    send(reconnect.pid, :reconnect)

    assert_receive :reconnect_started, 2_000
    assert_receive {:DOWN, ^connection_monitor, :process, ^old_connection, _reason}, 2_000
    assert_receive {:DOWN, ^listener_monitor, :process, ^old_listener, :shutdown}, 2_000
    assert {:close, 1013, "Relay capacity unavailable"} = recv_until_close(socket)

    {replacement, old_connection_alive?} = Task.await(reconnect, 5_000)
    refute old_connection_alive?
    track({:socket, replacement})

    new_capacity = await_replacement(PaseoRelay.Capacity, old_capacity)
    assert is_pid(new_capacity)
    assert runtime_child(PaseoRelay.Listener) != old_listener
    await_metric(:active_websockets, &(&1 == baseline + 1))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_reserved(&(&1 == 0))

    close_raw_websocket(replacement)
    await_metric(:active_websockets, &(&1 == baseline))
  end

  test "a missing daemon data route expires with an explicit retryable close" do
    port = start_relay(data_attach_timeout: 100)
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    {:ok, source} = connect(v2_url(port, "attach-timeout-#{port}", "client", "missing"))
    assert_receive {:relay_open, ^source}
    :ok = WebSockex.send_frame(source, {:text, "cannot-buffer"})

    assert_receive {:relay_closed, ^source, {:remote, 1013, "Data route unavailable"}}, 2_000
    await_metric(:active_websockets, &(&1 == baseline))
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 20_000
  test "destination death releases every blocked producer and reservation" do
    port = start_relay(send_timeout: 5_000)
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    server_id = "destination-death-#{port}"

    destination =
      raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")

    sources =
      Enum.map(1..5, fn _ ->
        raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
      end)

    await_metric(:active_websockets, &(&1 == baseline + 6))
    frame_count = PaseoRelay.Metrics.value(:frame_size_count)
    payload = :binary.copy(<<13>>, @frame_bytes)
    Enum.each(sources, fn source -> assert :ok = send_frame(source, :binary, payload) end)
    await_metric(:frame_size_count, &(&1 == frame_count + length(sources)))
    await_metric(:backpressured_sources, &(&1 >= 1))

    :gen_tcp.close(destination)

    Enum.each(sources, fn source ->
      assert {:close, code, reason} = recv_until_close(source)
      assert {code, reason} in [{1012, "Server disconnected"}, {1013, "Delivery unavailable"}]
    end)

    await_metric(:backpressured_sources, &(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    await_metric(:active_websockets, &(&1 == baseline))
    await_reserved(&(&1 == 0))
  end

  test "fanout metrics count every actual destination delivery" do
    port = start_relay()
    server_id = "fanout-metrics-#{port}"
    {:ok, first} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^first}
    {:ok, second} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^second}
    {:ok, daemon} = connect(v2_url(port, server_id, "server", "shared"))
    assert_receive {:relay_open, ^daemon}
    frames_baseline = PaseoRelay.Metrics.value(:frames_forwarded)
    bytes_baseline = PaseoRelay.Metrics.value(:bytes_forwarded)
    payload = "count-each-destination"

    :ok = WebSockex.send_frame(daemon, {:text, payload})
    assert_receive {:relay_frame, ^first, :text, ^payload}
    assert_receive {:relay_frame, ^second, :text, ^payload}
    await_metric(:frames_forwarded, &(&1 == frames_baseline + 2))
    await_metric(:bytes_forwarded, &(&1 == bytes_baseline + 2 * byte_size(payload)))

    Enum.each([first, second, daemon], &stop_resource({:client, &1}))
    await_metric(:active_websockets, &(&1 == 0))
  end

  @tag timeout: 45_000
  test "a real unread fanout peer reaches its send deadline without delaying healthy order" do
    # The Writer deadline fires first; the blocked real TCP send then reaches
    # its own transport deadline before the connection can finish cleanup.
    port = start_relay(delivery_timeout: 500, send_timeout: 1_000)
    server_id = "fanout-#{port}"
    active_baseline = PaseoRelay.Metrics.value(:active_websockets)
    slow = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    await_metric(:active_websockets, &(&1 == active_baseline + 1))
    {:ok, healthy} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^healthy}
    daemon = raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")
    await_metric(:active_websockets, &(&1 == active_baseline + 3))
    slow_baseline = PaseoRelay.Metrics.value(:slow_consumer_disconnects)
    send_ordered_pressure_frames(daemon, healthy, 1, 8)

    await_metric(:slow_consumer_disconnects, &(&1 == slow_baseline + 1))
    await_metric(:active_websockets, &(&1 == active_baseline + 2))

    :ok = send_frame(daemon, :text, "after-slow-client")
    assert_receive {:relay_frame, ^healthy, :text, "after-slow-client"}, 5_000
    await_metric(:backpressured_sources, &(&1 == 0))
    close_raw_websocket(daemon)
    close_raw(slow)
    stop_resource({:client, healthy})
    await_metric(:active_websockets, &(&1 == active_baseline))
  end

  # A peer may send its last messages and a Close in one TCP write. Cowboy parses the
  # whole buffer, so the Close reaches termination while those messages are still
  # being delivered or queued. Suspending the real Owner holds delivery open until
  # the source has answered the Close, which makes that interleaving deterministic.
  for version <- [2, 1], role <- [:server, :client], close <- [:coded, :empty] do
    destination_role = if role == :server, do: :client, else: :server
    close_frame = if close == :coded, do: Macro.escape({:close, 4401, "rejected"}), else: :close
    # v2 detaches the counterpart; v1 has no counterpart Close.
    counterpart_close = if version == 2, do: if(role == :server, do: 1012, else: 1001)

    test "v#{version} #{role} messages coalesced with a #{close} Close are delivered before detach" do
      version = unquote(version)
      port = start_relay(delivery_timeout: 2_000, send_timeout: 3_000)
      server_id = "close-drain-v#{version}-#{unquote(role)}-#{unquote(close)}-#{port}"
      source = raw_connect(port, route_path(version, server_id, unquote(role)))
      destination = raw_connect(port, route_path(version, server_id, unquote(destination_role)))
      owner = await_attached(server_id, 2)
      :ok = send_frame(source, :text, "ready")
      assert {:text, "ready"} = recv_server_frame(destination, 5_000)

      :ok = :sys.suspend(owner)
      on_exit(fn -> if Process.alive?(owner), do: :sys.resume(owner) end)
      frames = [{:text, "one"}, {:binary, "two"}, {:text, "three"}, unquote(close_frame)]
      :ok = :gen_tcp.send(source, Enum.map(frames, &:cow_ws.masked_frame(&1, 0x11223344)))

      # The source answers the Close before Owner can route any of the messages.
      assert_close_echo(source)
      :ok = :sys.resume(owner)

      assert {:text, "one"} = recv_server_frame(destination, 5_000)
      assert {:binary, "two"} = recv_server_frame(destination, 5_000)
      assert {:text, "three"} = recv_server_frame(destination, 5_000)
      assert_counterpart_close(destination, unquote(counterpart_close))
    end
  end

  test "a close drain stops at the first failed delivery" do
    port = start_relay(delivery_timeout: 2_000, data_attach_timeout: 100, send_timeout: 3_000)
    server_id = "close-drain-failure-#{port}"
    client = raw_connect(port, v2_path(server_id, "client", "shared"))
    owner = await_attached(server_id, 1)
    source = socket_pid(owner, :client)
    source_monitor = Process.monitor(source)
    :erlang.trace(owner, true, [:receive])
    on_exit(fn -> if Process.alive?(owner), do: :erlang.trace(owner, false, [:receive]) end)
    frames = [{:text, "one"}, {:text, "two"}, {:text, "three"}, {:close, 4000, "bye"}]
    :ok = :gen_tcp.send(client, Enum.map(frames, &:cow_ws.masked_frame(&1, 0x11223344)))
    assert_close_echo(client)

    # "one" fails at the 100 ms attach timeout. A drain that kept going would ask Owner
    # for a route once per queued message.
    assert_receive {:DOWN, ^source_monitor, :process, ^source, _reason}, 1_000
    :erlang.trace(owner, false, [:receive])
    assert route_lookups(owner, source) == 1
  end

  test "a close drain behind a stalled Owner ends at the in-flight deadline and retires it" do
    port = start_relay(delivery_timeout: 300, send_timeout: 1_000)
    server_id = "close-drain-deadline-#{port}"
    client = raw_connect(port, v2_path(server_id, "client", "shared"))
    daemon = raw_connect(port, v2_path(server_id, "server", "shared"))
    owner = await_attached(server_id, 2)
    :ok = send_frame(client, :text, "ready")
    assert {:text, "ready"} = recv_server_frame(daemon, 5_000)

    source = socket_pid(owner, :client)
    source_monitor = Process.monitor(source)
    owner_monitor = Process.monitor(owner)
    :ok = :sys.suspend(owner)
    on_exit(fn -> if Process.alive?(owner), do: :sys.resume(owner) end)
    frames = Enum.map(1..12, &{:text, "late-#{&1}"}) ++ [{:close, 4000, "bye"}]
    started = System.monotonic_time(:millisecond)
    :ok = :gen_tcp.send(client, Enum.map(frames, &:cow_ws.masked_frame(&1, 0x11223344)))

    # Owner.destinations/4 retires an Owner whose call reaches its deadline. The drain
    # must let that happen and then end, not wait for the queued messages.
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 1_500
    assert_receive {:DOWN, ^source_monitor, :process, ^source, _reason}, 1_500
    assert System.monotonic_time(:millisecond) - started < 1_000
  end

  test "a queued delivery shares the close deadline and still retires a stalled Owner" do
    port = start_relay(delivery_timeout: 600, send_timeout: 2_000)
    server_id = "close-drain-shared-deadline-#{port}"
    client = raw_connect(port, v2_path(server_id, "client", "shared"))
    daemon = raw_connect(port, v2_path(server_id, "server", "shared"))
    owner = await_attached(server_id, 2)
    :ok = send_frame(client, :text, "ready")
    assert {:text, "ready"} = recv_server_frame(daemon, 5_000)

    source = socket_pid(owner, :client)
    destination = socket_pid(owner, :server)
    source_monitor = Process.monitor(source)
    owner_monitor = Process.monitor(owner)

    on_exit(fn ->
      if Process.alive?(owner), do: :sys.resume(owner)
      if Process.alive?(destination), do: :sys.resume(destination)
    end)

    # Hold the first delivery at the destination's write barrier, then stall Owner so
    # the queued delivery's route lookup waits there.
    :ok = :sys.suspend(destination)
    frames = [{:text, "first"}, {:text, "queued"}, {:close, 4000, "bye"}]
    started = System.monotonic_time(:millisecond)
    :ok = :gen_tcp.send(client, Enum.map(frames, &:cow_ws.masked_frame(&1, 0x11223344)))
    assert_close_echo(client)
    await_mailbox(destination, &match?({:relay_write_barrier, _writer, _reference}, &1))
    :ok = :sys.suspend(owner)
    Process.sleep(250)
    :ok = :sys.resume(destination)
    assert {:text, "first"} = recv_server_frame(daemon, 2_000)
    await_mailbox(owner, &match?({:"$gen_call", _from, {:destinations, ^source, _, _}}, &1))

    # The queued lookup carries the drain's deadline, set when termination began just
    # after the send above. A fresh per-message deadline would be at least 250 ms later.
    {:messages, messages} = Process.info(owner, :messages)

    [{task, deadline}] =
      for {:"$gen_call", {task, _tag}, {:destinations, ^source, deadline, _}} <- messages,
          do: {task, deadline}

    assert deadline - started <= 600 + 100

    # Hold the delivery task past the deadline. A drain that timed out deliveries
    # itself would kill the task here, before it could retire the stalled Owner.
    :erlang.suspend_process(task)
    Process.sleep(max(deadline - System.monotonic_time(:millisecond), 0) + 150)
    assert Process.alive?(task), "the drain cancelled a delivery before it could retire Owner"
    :erlang.resume_process(task)

    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 1_500
    assert_receive {:DOWN, ^source_monitor, :process, ^source, _reason}, 1_000
    refute match?({:text, "queued"}, recv_server_frame(daemon, 500))
  end

  test "a queued delivery to a stalled Writer still ends with the drain" do
    port = start_relay(delivery_timeout: 600, send_timeout: 2_000)
    server_id = "close-drain-stalled-writer-#{port}"
    client = raw_connect(port, v2_path(server_id, "client", "shared"))
    daemon = raw_connect(port, v2_path(server_id, "server", "shared"))
    owner = await_attached(server_id, 2)
    :ok = send_frame(client, :text, "ready")
    assert {:text, "ready"} = recv_server_frame(daemon, 5_000)

    source = socket_pid(owner, :client)
    destination = socket_pid(owner, :server)
    writer = :sys.get_state(owner).sockets[destination].writer
    source_monitor = Process.monitor(source)

    on_exit(fn ->
      for pid <- [owner, destination, writer], Process.alive?(pid), do: :sys.resume(pid)
    end)

    # Complete the first delivery, hold the queued lookup at Owner, stall the Writer,
    # then release Owner so the queued reservation waits on the stalled Writer. The
    # drain has no timer of its own; the reservation's deadline must end it while
    # Owner stays healthy.
    :ok = :sys.suspend(destination)
    frames = [{:text, "first"}, {:text, "queued"}, {:close, 4000, "bye"}]
    :ok = :gen_tcp.send(client, Enum.map(frames, &:cow_ws.masked_frame(&1, 0x11223344)))
    assert_close_echo(client)
    await_mailbox(destination, &match?({:relay_write_barrier, _writer, _reference}, &1))
    :ok = :sys.suspend(owner)
    :ok = :sys.resume(destination)
    assert {:text, "first"} = recv_server_frame(daemon, 2_000)
    await_mailbox(owner, &match?({:"$gen_call", _from, {:destinations, ^source, _, _}}, &1))
    :ok = :sys.suspend(writer)
    :ok = :sys.resume(owner)
    await_mailbox(writer, &match?({:"$gen_call", _from, {:reserve, _, _}}, &1))

    assert_receive {:DOWN, ^source_monitor, :process, ^source, _reason}, 1_500
    assert Process.alive?(owner)
    assert Process.alive?(writer)

    await_transient_gauges(%{
      active_websockets: 1,
      backpressured_sources: 0,
      inflight_delivery_bytes: 0,
      ingress_reserved_bytes: 0
    })

    :ok = :sys.resume(writer)
    refute match?({:text, "queued"}, recv_server_frame(daemon, 500))
  end

  test "memory pressure interrupts a close drain" do
    port = start_relay(delivery_timeout: 5_000, send_timeout: 6_000)
    server_id = "close-drain-pressure-#{port}"
    client = raw_connect(port, v2_path(server_id, "client", "shared"))
    daemon = raw_connect(port, v2_path(server_id, "server", "shared"))
    owner = await_attached(server_id, 2)
    :ok = send_frame(client, :text, "ready")
    assert {:text, "ready"} = recv_server_frame(daemon, 5_000)

    source = socket_pid(owner, :client)
    source_monitor = Process.monitor(source)
    :ok = :sys.suspend(owner)
    on_exit(fn -> if Process.alive?(owner), do: :sys.resume(owner) end)
    frames = [{:text, "one"}, {:text, "two"}, {:close, 4000, "bye"}]
    :ok = :gen_tcp.send(client, Enum.map(frames, &:cow_ws.masked_frame(&1, 0x11223344)))
    assert_close_echo(client)

    # The drain is live: the source waits on its first route lookup with seconds left.
    await_mailbox(owner, &match?({:"$gen_call", _from, {:destinations, ^source, _, _}}, &1))
    assert Process.alive?(source)
    send(source, :relay_memory_pressure)
    assert_receive {:DOWN, ^source_monitor, :process, ^source, _reason}, 1_000
    :ok = :sys.resume(owner)
    refute match?({:text, _payload}, recv_server_frame(daemon, 500))
  end

  defp start_relay(options \\ []) do
    port = available_port()
    reference = {:backpressure, System.unique_integer([:positive])}

    config =
      PaseoRelay.Config.normalize(%{
        PaseoRelay.Config.defaults()
        | delivery_timeout_ms: Keyword.get(options, :delivery_timeout, 30_000),
          transport_send_timeout_ms: Keyword.get(options, :send_timeout, 35_000),
          control_queue_bytes: Keyword.get(options, :control_queue, 1024 * 1024),
          data_attach_timeout_ms: Keyword.get(options, :data_attach_timeout, 15_000),
          websocket_max_heap_words:
            Keyword.get(
              options,
              :websocket_heap_words,
              PaseoRelay.Config.defaults().websocket_max_heap_words
            )
      })

    relay =
      start_supervised!(
        {PaseoRelay.Listener,
         ref: reference,
         config: config,
         ip: {127, 0, 0, 1},
         port: port,
         acceptors: 4,
         max_connections: 1_000,
         send_timeout_ms: Keyword.get(options, :send_timeout, 35_000),
         send_buffer_bytes: Keyword.get(options, :send_buffer, 1024)}
      )

    track({:listener, relay})
    port
  end

  defp raw_connect(port, path, options \\ []) do
    {:ok, socket, response} = upgrade_once(port, path, options)
    assert response =~ "HTTP/1.1 101 Switching Protocols"
    track({:socket, socket})
    socket
  end

  defp upgrade_once(port, path, options \\ []) do
    with {:ok, socket} <-
           :gen_tcp.connect(~c"127.0.0.1", port, [
             :binary,
             active: false,
             nodelay: true,
             recbuf: Keyword.get(options, :receive_buffer, 1024),
             send_timeout: 10_000
           ]),
         key = Base.encode64(:crypto.strong_rand_bytes(16)),
         request =
           "GET #{path} HTTP/1.1\r\n" <>
             "Host: relay.test\r\n" <>
             "Upgrade: websocket\r\n" <>
             "Connection: Upgrade\r\n" <>
             "Sec-WebSocket-Version: 13\r\n" <>
             "Sec-WebSocket-Key: #{key}\r\n\r\n",
         :ok <- :gen_tcp.send(socket, request),
         {:ok, response} <- recv_headers(socket, "") do
      {:ok, socket, response}
    end
  end

  defp http_get(port, path) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, "GET #{path} HTTP/1.0\r\nHost: relay.test\r\n\r\n")
    {:ok, response} = recv_headers(socket, "")
    :ok = :gen_tcp.close(socket)
    response
  end

  defp connect(url, owner \\ nil) do
    {:ok, client} = RelayClient.start_link(url, owner || self())
    Process.unlink(client)
    track({:client, client})
    {:ok, client}
  end

  defp digest_connect(url) do
    {:ok, client} = DigestClient.start_link(url, self())
    Process.unlink(client)
    track({:client, client})
    assert_receive {:digest_open, ^client}
    client
  end

  defp v2_url(port, server_id, role, connection_id) do
    "ws://127.0.0.1:#{port}/ws?serverId=#{server_id}&role=#{role}&v=2&connectionId=#{connection_id}"
  end

  defp v2_path(server_id, role, connection_id) do
    "/ws?serverId=#{server_id}&role=#{role}&v=2&connectionId=#{connection_id}"
  end

  defp route_path(1, server_id, role), do: "/ws?serverId=#{server_id}&role=#{role}"
  defp route_path(2, server_id, role), do: v2_path(server_id, role, "shared")

  # Cowboy answers an empty Close with an empty Close, which recv_server_frame/2
  # cannot read once the server has closed the transport.
  defp assert_close_echo(socket) do
    assert {:ok, <<0x88, length>>} = :gen_tcp.recv(socket, 2, 2_000)
    if length > 0, do: assert({:ok, _payload} = :gen_tcp.recv(socket, length, 2_000))
  end

  # HTTP 101 can precede Owner attachment, and v1 routes only to attached sockets.
  defp await_attached(server_id, count) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_attached(server_id, count, deadline)
  end

  defp await_attached(server_id, count, deadline) do
    owner = PaseoRelay.Ownership.owner_pid(server_id)

    cond do
      is_pid(owner) and map_size(:sys.get_state(owner).sockets) >= count ->
        owner

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("#{server_id} never attached #{count} sockets")

      true ->
        Process.sleep(10)
        await_attached(server_id, count, deadline)
    end
  end

  defp await_mailbox(pid, predicate) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    await_mailbox(pid, predicate, deadline)
  end

  defp await_mailbox(pid, predicate, deadline) do
    {:messages, messages} = Process.info(pid, :messages)

    cond do
      Enum.any?(messages, predicate) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("#{inspect(pid)} never received the expected message")

      true ->
        Process.sleep(5)
        await_mailbox(pid, predicate, deadline)
    end
  end

  # Counts the route lookups a traced Owner received from one socket, after the
  # runtime confirms every trace message for Owner has been delivered.
  defp route_lookups(owner, socket) do
    reference = :erlang.trace_delivered(owner)
    assert_receive {:trace_delivered, ^owner, ^reference}, 2_000
    count_route_lookups(owner, socket)
  end

  defp count_route_lookups(owner, socket) do
    receive do
      {:trace, ^owner, :receive, {:"$gen_call", _from, {:destinations, ^socket, _, _}}} ->
        1 + count_route_lookups(owner, socket)

      {:trace, ^owner, :receive, _message} ->
        count_route_lookups(owner, socket)
    after
      0 -> 0
    end
  end

  defp assert_counterpart_close(_socket, nil), do: :ok

  defp assert_counterpart_close(socket, code) do
    assert {:close, ^code, _reason} = recv_server_frame(socket, 5_000)
  end

  defp socket_pid(owner, role) do
    {pid, _socket} =
      owner
      |> :sys.get_state()
      |> Map.fetch!(:sockets)
      |> Enum.find(fn {_pid, %{connection: connection}} -> connection.role == role end)

    pid
  end

  defp recv_headers(socket, acc) do
    if String.contains?(acc, "\r\n\r\n") do
      {:ok, acc}
    else
      with {:ok, data} <- :gen_tcp.recv(socket, 0, 2_000) do
        recv_headers(socket, acc <> data)
      end
    end
  end

  defp send_frame(socket, opcode, payload) do
    opcode = if opcode == :text, do: 0x1, else: 0x2
    send_raw_frame(socket, opcode, payload, true)
  end

  defp send_frames(socket, frames) do
    encoded =
      Enum.map(frames, fn
        {:text, payload} -> :cow_ws.masked_frame({:text, payload}, 0x11223344)
        {:binary, payload} -> :cow_ws.masked_frame({:binary, payload}, 0x11223344)
      end)

    :gen_tcp.send(socket, encoded)
  end

  defp send_ordered_pressure_frames(_source, _destination, sequence, last)
       when sequence > last,
       do: :ok

  defp send_ordered_pressure_frames(source, destination, sequence, last) do
    payload =
      <<sequence::32, :binary.copy(<<rem(sequence, 256)>>, @pressure_frame_bytes - 4)::binary>>

    assert :ok = send_frame(source, :binary, payload)
    assert_receive {:relay_frame, ^destination, :binary, ^payload}, 15_000
    await_metric(:backpressured_sources, &(&1 == 0))
    send_ordered_pressure_frames(source, destination, sequence + 1, last)
  end

  defp send_raw_frame(socket, opcode, payload, fin) do
    first = if(fin, do: 0x80, else: 0x00) ||| opcode

    length =
      case byte_size(payload) do
        value when value <= 125 -> <<0x80 ||| value>>
        value when value <= 65_535 -> <<0x80 ||| 126, value::16>>
        value -> <<0x80 ||| 127, value::64>>
      end

    # A zero masking key is valid and leaves these large test payloads unchanged.
    :gen_tcp.send(socket, [<<first>>, length, <<0::32>>, payload])
  end

  defp send_frame_header(socket, opcode, length) do
    opcode = if opcode == :text, do: 0x1, else: 0x2
    :gen_tcp.send(socket, <<0x80 ||| opcode, 0x80 ||| 127, length::64, 0x11223344::32>>)
  end

  defp recv_server_frame(socket, timeout \\ 30_000) do
    case :gen_tcp.recv(socket, 2, timeout) do
      {:ok, <<first, second>>} -> decode_server_frame(socket, first, second, timeout)
      {:error, reason} -> {:transport_error, reason}
    end
  end

  defp decode_server_frame(socket, first, second, timeout) do
    opcode = first &&& 0x0F

    with {:ok, length} <- server_frame_length(socket, second &&& 0x7F, timeout),
         {:ok, payload} <- :gen_tcp.recv(socket, length, timeout) do
      case {opcode, payload} do
        {0x8, <<code::16, reason::binary>>} -> {:close, code, reason}
        {0xA, payload} -> {:pong, payload}
        {0x1, payload} -> {:text, payload}
        {0x2, payload} -> {:binary, payload}
      end
    else
      {:error, reason} -> {:transport_error, reason}
    end
  end

  defp server_frame_length(socket, 126, timeout) do
    case :gen_tcp.recv(socket, 2, timeout) do
      {:ok, <<value::16>>} -> {:ok, value}
      error -> error
    end
  end

  defp server_frame_length(socket, 127, timeout) do
    case :gen_tcp.recv(socket, 8, timeout) do
      {:ok, <<value::64>>} -> {:ok, value}
      error -> error
    end
  end

  defp server_frame_length(_socket, value, _timeout), do: {:ok, value}

  defp recv_until_close(socket) do
    case recv_server_frame(socket) do
      {:close, _code, _reason} = close -> close
      {:transport_error, _reason} = error -> error
      _frame -> recv_until_close(socket)
    end
  end

  defp recv_control_type(socket, type) do
    case recv_server_frame(socket) do
      {:text, payload} ->
        case Jason.decode(payload) do
          {:ok, %{"type" => ^type} = message} -> message
          _other -> recv_control_type(socket, type)
        end

      _other ->
        recv_control_type(socket, type)
    end
  end

  defp await_transport_close(socket, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_transport_close_until(socket, deadline)
  end

  defp await_transport_close_until(socket, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    case :gen_tcp.recv(socket, 0, min(remaining, 2_000)) do
      {:error, :closed} -> :ok
      {:ok, _bytes} -> await_transport_close_until(socket, deadline)
      {:error, :timeout} when remaining > 0 -> await_transport_close_until(socket, deadline)
      {:error, reason} -> {:error, reason}
    end
  end

  defp writer_processes do
    Process.list()
    |> Enum.filter(fn process ->
      case Process.info(process, :dictionary) do
        {:dictionary, dictionary} ->
          dictionary[:"$initial_call"] == {PaseoRelay.Delivery.Writer, :init, 1}

        nil ->
          false
      end
    end)
    |> MapSet.new()
  end

  defp ranch_connections(port) do
    reference =
      :ranch.info()
      |> Enum.find_value(fn {reference, info} ->
        if info.port == port, do: reference
      end)

    :ranch.procs(reference, :connections)
  end

  defp resume_process(process) do
    if Process.alive?(process) do
      try do
        :erlang.resume_process(process)
      catch
        :error, :badarg -> :ok
      end
    end
  end

  defp await_new_writer(existing) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    await_new_writer(existing, deadline)
  end

  defp await_new_writer(existing, deadline) do
    writers = writer_processes() |> MapSet.difference(existing) |> MapSet.to_list()

    cond do
      match?([_writer], writers) ->
        hd(writers)

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("expected one new Writer, got #{inspect(writers)}")

      true ->
        receive do
        after
          10 -> await_new_writer(existing, deadline)
        end
    end
  end

  defp await_replacement(name, old_process) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    await_replacement(name, old_process, deadline)
  end

  defp await_replacement(name, old_process, deadline) do
    case Process.whereis(name) do
      replacement when is_pid(replacement) and replacement != old_process ->
        replacement

      _missing_or_old ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("#{inspect(name)} was not replaced")
        end

        Process.sleep(10)
        await_replacement(name, old_process, deadline)
    end
  end

  defp runtime_child(id) do
    PaseoRelay.RuntimeSupervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^id, process, _type, _modules} -> process
      _other -> nil
    end)
  end

  defp await_runtime_child_replacement(id, old_process) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    await_runtime_child_replacement(id, old_process, deadline)
  end

  defp await_runtime_child_replacement(id, old_process, deadline) do
    case runtime_child(id) do
      replacement when is_pid(replacement) and replacement != old_process ->
        replacement

      _missing_or_old ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("#{inspect(id)} was not replaced")
        end

        Process.sleep(10)
        await_runtime_child_replacement(id, old_process, deadline)
    end
  end

  defp await_ranch_connection(existing) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    await_ranch_connection(existing, deadline)
  end

  defp await_ranch_connection(existing, deadline) do
    connections =
      PaseoRelay.Listener
      |> :ranch.procs(:connections)
      |> MapSet.new()
      |> MapSet.difference(existing)
      |> MapSet.to_list()

    cond do
      match?([_connection], connections) ->
        hd(connections)

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("expected one Ranch connection, got #{inspect(connections)}")

      true ->
        Process.sleep(10)
        await_ranch_connection(existing, deadline)
    end
  end

  defp reconnect_until_up(port, path, old_connection) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    reconnect_until_up(port, path, old_connection, deadline)
  end

  defp reconnect_until_up(port, path, old_connection, deadline) do
    case upgrade_once(port, path) do
      {:ok, socket, "HTTP/1.1 101" <> _response} ->
        {socket, Process.alive?(old_connection)}

      {:ok, socket, _response} ->
        :gen_tcp.close(socket)
        retry_reconnect(port, path, old_connection, deadline)

      {:error, _reason} ->
        retry_reconnect(port, path, old_connection, deadline)
    end
  end

  defp retry_reconnect(port, path, old_connection, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      flunk("production listener did not reopen")
    end

    Process.sleep(10)
    reconnect_until_up(port, path, old_connection, deadline)
  end

  defp reconnect_after_pressure(port, path) do
    reconnect_after_pressure(port, path, System.monotonic_time(:millisecond) + 5_000)
  end

  defp reconnect_after_pressure(port, path, deadline) do
    case upgrade_once(port, path) do
      {:ok, socket, "HTTP/1.1 101" <> _response} ->
        track({:socket, socket})
        socket

      {:ok, socket, _response} ->
        :gen_tcp.close(socket)
        retry_after_pressure(port, path, deadline)

      {:error, _reason} ->
        retry_after_pressure(port, path, deadline)
    end
  end

  defp retry_after_pressure(port, path, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      flunk("capacity admission did not reopen after measured memory recovery")
    end

    Process.sleep(10)
    reconnect_after_pressure(port, path, deadline)
  end

  defp close_raw(socket) do
    :gen_tcp.close(socket)
    :ok
  end

  defp close_raw_websocket(socket) do
    assert :ok = send_raw_frame(socket, 0x8, <<1000::16>>, true)
    assert {:close, 1000, _reason} = recv_until_close(socket)
    close_raw(socket)
  end

  defp track(resource) do
    resources = Process.get(:relay_test_resources) || raise "missing relay test resource tracker"
    Agent.update(resources, &[resource | &1])
    resource
  end

  defp stop_resource({:socket, socket}), do: close_raw(socket)

  defp stop_resource({:client, pid}) do
    if Process.alive?(pid) do
      reference = Process.monitor(pid)
      WebSockex.cast(pid, :close)

      receive do
        {:DOWN, ^reference, :process, ^pid, _reason} -> :ok
      after
        5_000 -> flunk("client #{inspect(pid)} did not close its WebSocket synchronously")
      end
    end
  end

  defp stop_resource({_kind, pid}), do: stop_process(pid)

  defp stop_process(pid) do
    if Process.alive?(pid) do
      reference = Process.monitor(pid)

      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end

      receive do
        {:DOWN, ^reference, :process, ^pid, _reason} -> :ok
      after
        5_000 -> flunk("resource #{inspect(pid)} did not stop synchronously")
      end
    end
  end

  defp reconfigure_pressure(watermark) do
    PaseoRelay.Capacity.set_watermark(watermark, @capacity_mutation_timeout_ms)
  end

  defp transient_gauges do
    Map.new(
      [
        :active_websockets,
        :backpressured_sources,
        :inflight_delivery_bytes,
        :ingress_reserved_bytes
      ],
      fn name ->
        {name, PaseoRelay.Metrics.value(name)}
      end
    )
  end

  defp await_transient_gauges(expected) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_transient_gauges(expected, deadline)
  end

  defp await_transient_gauges(expected, deadline) do
    actual = transient_gauges()

    cond do
      actual == expected and
          PaseoRelay.Capacity.value(:ingress_reserved_bytes) == expected.ingress_reserved_bytes ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("transient gauges remained #{inspect(actual)}; expected #{inspect(expected)}")

      true ->
        Process.sleep(10)
        await_transient_gauges(expected, deadline)
    end
  end

  defp await_metric(name, predicate) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_metric(name, predicate, deadline)
  end

  defp await_unowned(server_id, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 1_000

    if PaseoRelay.Ownership.resolve(server_id) == :unowned do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("#{server_id} remained owned")
      end

      Process.sleep(10)
      await_unowned(server_id, deadline)
    end
  end

  defp await_metric(name, predicate, deadline) do
    value = PaseoRelay.Metrics.value(name)

    cond do
      predicate.(value) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("#{name} remained #{value}")

      true ->
        Process.sleep(10)
        await_metric(name, predicate, deadline)
    end
  end

  defp await_stable_metric(name, predicate, stable_ms) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_stable_metric(name, predicate, stable_ms, deadline)
  end

  defp await_stable_metric(name, predicate, stable_ms, deadline) do
    await_metric(name, predicate, deadline)
    Process.sleep(stable_ms)

    if predicate.(PaseoRelay.Metrics.value(name)) do
      :ok
    else
      await_stable_metric(name, predicate, stable_ms, deadline)
    end
  end

  defp await_reserved(predicate) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_reserved(predicate, deadline)
  end

  defp await_reserved(predicate, deadline) do
    value = PaseoRelay.Capacity.value(:ingress_reserved_bytes)

    cond do
      predicate.(value) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("reserved bytes remained #{value}")

      true ->
        Process.sleep(10)
        await_reserved(predicate, deadline)
    end
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
