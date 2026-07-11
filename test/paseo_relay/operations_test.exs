defmodule PaseoRelay.OperationsTest do
  use ExUnit.Case, async: false
  import Plug.Test

  alias PaseoRelay.Operations

  setup do
    PaseoRelay.Drain.cancel()
    on_exit(&PaseoRelay.Drain.cancel/0)
    :ok
  end

  test "health is live while readiness refuses new work during a drain" do
    PaseoRelay.Drain.begin()
    live = Operations.call(conn(:get, "/health"), [])
    draining = Operations.call(conn(:get, "/ready"), [])

    assert {live.status, live.resp_body} == {200, ~s({"status":"ok"})}
    assert {draining.status, draining.resp_body} == {503, ~s({"status":"unready"})}
  end

  test "metrics expose a stable Prometheus surface before relay wiring exists" do
    metrics = Operations.call(conn(:get, "/metrics"), [])

    assert metrics.status == 200
    assert metrics.resp_body =~ "# TYPE paseo_relay_ready gauge"
    assert metrics.resp_body =~ "# TYPE paseo_relay_draining gauge"
    assert metrics.resp_body =~ "# TYPE paseo_relay_active_websockets gauge"
    assert metrics.resp_body =~ "# TYPE paseo_relay_active_sessions gauge"
    assert metrics.resp_body =~ "# TYPE paseo_relay_reroute_responses_total counter"
    assert metrics.resp_body =~ "# TYPE paseo_relay_connection_rejections_total counter"
    assert metrics.resp_body =~ "# TYPE paseo_relay_frames_forwarded_total counter"
    assert metrics.resp_body =~ "# TYPE paseo_relay_bytes_forwarded_total counter"
    assert metrics.resp_body =~ "paseo_relay_ready 1"
    assert metrics.resp_body =~ "paseo_relay_draining 0"
  end

  test "readiness and its metric stay false until the configured cluster floor is present" do
    visible_cluster_size =
      length(:syn.subcluster_nodes(:registry, :paseo_relay_owners)) + 1

    Application.put_env(:paseo_relay, :minimum_cluster_size, visible_cluster_size + 1)
    on_exit(fn -> Application.put_env(:paseo_relay, :minimum_cluster_size, 1) end)

    readiness = Operations.call(conn(:get, "/ready"), [])
    metrics = Operations.call(conn(:get, "/metrics"), [])

    assert {readiness.status, readiness.resp_body} == {503, ~s({"status":"unready"})}
    assert metrics.resp_body =~ "paseo_relay_ready 0"
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
    response = Operations.call(conn(:get, "/metrics"), [])

    assert Process.alive?(supervisor)
    assert Process.alive?(replacement)
    assert response.status == 200
    assert PaseoRelay.Metrics.value(:reroute_responses) == reroutes_before_failure
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
end
