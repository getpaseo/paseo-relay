defmodule PaseoRelay.OperationsTest do
  use ExUnit.Case, async: false
  import Plug.Test

  alias PaseoRelay.Operations

  setup do
    PaseoRelay.Drain.cancel()
    :ok
  end

  test "health is live while readiness refuses new work during a drain" do
    PaseoRelay.Drain.begin()
    live = Operations.call(conn(:get, "/health"), [])
    draining = Operations.call(conn(:get, "/ready"), [])

    assert {live.status, live.resp_body} == {200, ~s({"status":"ok"})}
    assert {draining.status, draining.resp_body} == {503, ~s({"status":"draining"})}
  end

  test "metrics expose a stable Prometheus surface before relay wiring exists" do
    metrics = Operations.call(conn(:get, "/metrics"), [])

    assert metrics.status == 200
    assert metrics.resp_body =~ "paseo_relay_ready 1"
    assert metrics.resp_body =~ "paseo_relay_draining 0"
  end
end
