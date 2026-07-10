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
    assert metrics.resp_body =~ "paseo_relay_ready 1"
    assert metrics.resp_body =~ "paseo_relay_draining 0"
  end

  test "readiness and its metric stay false until the configured cluster floor is present" do
    Application.put_env(:paseo_relay, :minimum_cluster_size, 2)
    on_exit(fn -> Application.put_env(:paseo_relay, :minimum_cluster_size, 1) end)

    readiness = Operations.call(conn(:get, "/ready"), [])
    metrics = Operations.call(conn(:get, "/metrics"), [])

    assert {readiness.status, readiness.resp_body} == {503, ~s({"status":"unready"})}
    assert metrics.resp_body =~ "paseo_relay_ready 0"
  end
end
