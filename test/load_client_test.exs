defmodule PaseoRelay.LoadClientTest do
  use ExUnit.Case, async: true

  test "the black-box client documents generic v2 websocket roles" do
    {output, status} = System.cmd("node", ["scripts/relay-load.mjs", "--help"])

    assert status == 0
    assert output =~ "serverId"
    assert output =~ "connectionId"
    assert output =~ "--endpoints"
  end
end
