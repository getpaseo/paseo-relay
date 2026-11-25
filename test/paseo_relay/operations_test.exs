defmodule PaseoRelay.OperationsTest do
  use ExUnit.Case, async: false

  alias PaseoRelay.Operations

  setup do
    PaseoRelay.Drain.cancel()
    on_exit(&PaseoRelay.Drain.cancel/0)
    :ok
  end

  test "health is live while readiness refuses new work during a drain" do
    PaseoRelay.Drain.begin()
    live = Operations.response("/health")
    draining = Operations.response("/ready")

    assert {elem(live, 0), elem(live, 2)} == {200, ~s({"status":"ok"})}
    assert {elem(draining, 0), elem(draining, 2)} == {503, ~s({"status":"unready"})}
  end

  test "metrics expose a stable Prometheus surface before relay wiring exists" do
    {status, _content_type, metrics} = Operations.response("/metrics")

    assert status == 200
    assert metrics =~ "# TYPE paseo_relay_ready gauge"
    assert metrics =~ "# TYPE paseo_relay_draining gauge"
    assert metrics =~ "# TYPE paseo_relay_active_websockets gauge"
    assert metrics =~ "# TYPE paseo_relay_active_sessions gauge"
    assert metrics =~ "# TYPE paseo_relay_reroute_responses_total counter"
    assert metrics =~ "# TYPE paseo_relay_frames_forwarded_total counter"
    assert metrics =~ "# TYPE paseo_relay_bytes_forwarded_total counter"
    assert metrics =~ "# TYPE paseo_relay_ingress_reserved_bytes gauge"
    assert metrics =~ "# TYPE paseo_relay_inflight_delivery_bytes gauge"
    assert metrics =~ "# TYPE paseo_relay_backpressured_sources gauge"
    assert metrics =~ "# TYPE paseo_relay_delivery_wait_seconds histogram"
    assert metrics =~ "# TYPE paseo_relay_frame_size_bytes histogram"
    assert metrics =~ "# TYPE paseo_relay_beam_binary_memory_bytes gauge"
    assert metrics =~ "# TYPE paseo_relay_memory_watermark_bytes gauge"
    assert metrics =~ "# TYPE paseo_relay_misplaced_sessions gauge"
    assert metrics =~ "# TYPE paseo_relay_sessions_healed_total counter"
    assert metrics =~ "# TYPE paseo_relay_home_promotions_total counter"
    assert metrics =~ "paseo_relay_ready 1"
    assert metrics =~ "paseo_relay_draining 0"
  end

  test "readiness and its metric stay false until the configured cluster floor is present" do
    visible_cluster_size =
      length(:syn.subcluster_nodes(:registry, :paseo_relay_owners)) + 1

    config = %{
      PaseoRelay.Config.defaults()
      | minimum_cluster_size: visible_cluster_size + 1
    }

    readiness = Operations.response("/ready", config)
    {_status, _content_type, metrics} = Operations.response("/metrics", config)

    assert {elem(readiness, 0), elem(readiness, 2)} == {503, ~s({"status":"unready"})}
    assert metrics =~ "paseo_relay_ready 0"
  end

  test "metrics recovers from an abrupt process failure without taking down the relay" do
    supervisor = Process.whereis(PaseoRelay.Supervisor)
    metrics = Process.whereis(PaseoRelay.Metrics)
    metrics_down = Process.monitor(metrics)
    PaseoRelay.Metrics.inc(:reroute_responses)
    reroutes_before_failure = PaseoRelay.Metrics.value(:reroute_responses)

    Process.exit(metrics, :kill)

    assert_receive {:DOWN, ^metrics_down, :process, ^metrics, :killed}
    replacement = await_metrics_replacement(metrics)
    response = Operations.response("/metrics")

    assert Process.alive?(supervisor)
    assert Process.alive?(replacement)
    assert elem(response, 0) == 200
    assert PaseoRelay.Metrics.value(:reroute_responses) == reroutes_before_failure
  end

  @tag timeout: 5_000
  test "readiness is bounded and unavailable while Capacity is stalled" do
    capacity = Process.whereis(PaseoRelay.Capacity)
    :ok = :sys.suspend(capacity)

    on_exit(fn ->
      if Process.alive?(capacity), do: :sys.resume(capacity)
    end)

    started = System.monotonic_time(:millisecond)
    response = http_get("/ready", 1_500)
    elapsed = System.monotonic_time(:millisecond) - started

    assert response =~ "HTTP/1.1 503 Service Unavailable"
    assert response =~ ~s({"status":"unready"})
    assert elapsed < 1_500

    :ok = :sys.resume(capacity)
    recovered = http_get("/ready")
    assert recovered =~ "HTTP/1.1 200 OK"
    assert recovered =~ ~s({"status":"ready"})
  end

  @tag timeout: 5_000
  test "metrics omits unavailable Capacity gauges and retains independent telemetry" do
    websocket = open_websocket("stalled-metrics-established")

    on_exit(fn ->
      :gen_tcp.close(websocket)
      await_no_active_websockets()
    end)

    PaseoRelay.Metrics.inc(:reroute_responses)
    reroutes = PaseoRelay.Metrics.value(:reroute_responses)
    capacity = Process.whereis(PaseoRelay.Capacity)
    :ok = :sys.suspend(capacity)

    on_exit(fn ->
      if Process.alive?(capacity), do: :sys.resume(capacity)
    end)

    started = System.monotonic_time(:millisecond)
    metrics = http_get("/metrics", 1_500)
    elapsed = System.monotonic_time(:millisecond) - started

    assert metrics =~ "HTTP/1.1 200 OK"
    assert metrics =~ "paseo_relay_ready 0"
    assert elapsed < 1_500
    refute metrics =~ "paseo_relay_active_websockets"
    refute metrics =~ "paseo_relay_ingress_reserved_bytes"
    refute metrics =~ "paseo_relay_inflight_delivery_bytes"
    refute metrics =~ "paseo_relay_backpressured_sources"
    refute metrics =~ "paseo_relay_memory_watermark_bytes"
    assert metrics =~ "paseo_relay_reroute_responses_total #{reroutes}"
    assert metrics =~ "paseo_relay_active_sessions"
    assert metrics =~ "paseo_relay_delivery_wait_seconds"
    assert metrics =~ "paseo_relay_beam_binary_memory_bytes"

    :ok = :sys.resume(capacity)
    :ok = :gen_tcp.close(websocket)
  end

  defp open_websocket(server_id) do
    port = PaseoRelay.Listener.port(PaseoRelay.Listener)
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
    assert {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    assert response =~ "HTTP/1.1 101 Switching Protocols"
    socket
  end

  defp http_get(path, timeout \\ 2_000) do
    port = PaseoRelay.Listener.port(PaseoRelay.Listener)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, "GET #{path} HTTP/1.0\r\nHost: relay.test\r\n\r\n")
    response = recv_all(socket, System.monotonic_time(:millisecond) + timeout, [])
    :ok = :gen_tcp.close(socket)
    response
  end

  defp recv_all(socket, deadline, chunks) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    case :gen_tcp.recv(socket, 0, remaining) do
      {:ok, data} -> recv_all(socket, deadline, [data | chunks])
      {:error, :closed} -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
      {:error, :timeout} -> flunk("HTTP response did not finish before the probe deadline")
    end
  end

  defp await_metrics_replacement(previous) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    await_metrics_replacement(previous, deadline)
  end

  defp await_metrics_replacement(previous, deadline) do
    case Process.whereis(PaseoRelay.Metrics) do
      replacement when is_pid(replacement) and replacement != previous ->
        replacement

      _missing ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("metrics did not restart")
        end

        Process.sleep(10)
        await_metrics_replacement(previous, deadline)
    end
  end

  defp await_no_active_websockets do
    deadline = System.monotonic_time(:millisecond) + 2_000
    await_no_active_websockets(deadline)
  end

  defp await_no_active_websockets(deadline) do
    cond do
      PaseoRelay.Metrics.value(:active_websockets) == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("active WebSocket gauge did not return to zero")

      true ->
        receive do
        after
          10 -> await_no_active_websockets(deadline)
        end
    end
  end
end
