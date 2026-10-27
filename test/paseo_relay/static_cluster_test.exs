defmodule PaseoRelay.StaticClusterTest do
  use ExUnit.Case, async: false

  test "static peers are parsed, connected, and reconnected without DNS discovery" do
    {:ok, peer, peer_node} =
      :peer.start(%{
        name: :"static_peer_#{System.unique_integer([:positive])}",
        connection: :standard_io,
        args: [~c"+S", ~c"2", ~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
      })

    on_exit(fn -> :peer.stop(peer) end)
    refute peer_node in Node.list()

    {:ok, config} =
      PaseoRelay.Config.load(%{
        "PASEO_RELAY_CLUSTER_NODES" => " #{peer_node}, #{node()},#{peer_node} "
      })

    assert config.cluster_nodes == [peer_node, node()]
    start_supervised!({PaseoRelay.StaticCluster, nodes: config.cluster_nodes, interval: 50})
    await_connection(peer_node)
    assert Node.disconnect(peer_node)
    await_connection(peer_node)
  end

  test "the static peer boundary rejects malformed node names" do
    assert {:ok, %{cluster_nodes: []}} = PaseoRelay.Config.load(%{})

    for value <- ["not-a-node", "name@host@extra", "name@bad host"] do
      assert {:error, "PASEO_RELAY_CLUSTER_NODES" <> _} =
               PaseoRelay.Config.load(%{"PASEO_RELAY_CLUSTER_NODES" => value})
    end
  end

  defp await_connection(peer_node, attempts \\ 100)
  defp await_connection(_peer_node, 0), do: flunk("static peer did not connect")

  defp await_connection(peer_node, attempts) do
    unless peer_node in Node.list() do
      Process.sleep(20)
      await_connection(peer_node, attempts - 1)
    end
  end
end
