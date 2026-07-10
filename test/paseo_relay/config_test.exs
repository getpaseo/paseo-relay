defmodule PaseoRelay.ConfigTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.Config

  test "loads generic release settings with safe local defaults" do
    assert Config.load([]) ==
             {:ok,
              %{
                host: "127.0.0.1",
                ip: {127, 0, 0, 1},
                port: 4000,
                drain: false,
                node_name: nil,
                cookie: nil
              }}
  end

  test "rejects a listener hostname that the socket layer cannot bind" do
    assert Config.load([{"PASEO_RELAY_HOST", "not-an-ip"}]) ==
             {:error, "PASEO_RELAY_HOST must be an IP address"}
  end

  test "rejects an invalid port instead of starting on an unintended listener" do
    assert Config.load([{"PASEO_RELAY_PORT", "not-a-port"}]) ==
             {:error, "PASEO_RELAY_PORT must be an integer between 1 and 65535"}
  end

  test "recognizes drain mode from the release environment" do
    assert {:ok, %{drain: true, port: 4400}} =
             Config.load([{"PASEO_RELAY_DRAIN", "true"}, {"PASEO_RELAY_PORT", "4400"}])
  end
end
