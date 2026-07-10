defmodule PaseoRelay.RouterIntegrationTest do
  use ExUnit.Case, async: false

  setup do
    {:ok, listener} = Bandit.start_link(plug: PaseoRelay.Router, ip: {127, 0, 0, 1}, port: 0)
    Process.unlink(listener)
    {:ok, {_address, port}} = ThousandIsland.listener_info(listener)
    on_exit(fn -> if Process.alive?(listener), do: Supervisor.stop(listener) end)
    %{port: port}
  end

  test "a locally owned websocket request upgrades", %{port: port} do
    {socket, response} = open_websocket(port, "srv_local")
    assert "HTTP/1.1 101" <> _ = response
    :gen_tcp.close(socket)
  end

  test "health is live while readiness blocks new websocket ownership", %{port: port} do
    Application.put_env(:paseo_relay, :minimum_cluster_size, 2)
    on_exit(fn -> Application.delete_env(:paseo_relay, :minimum_cluster_size) end)

    assert "HTTP/1.1 200" <> _ = request(port, "/health")
    assert "HTTP/1.1 503" <> _ = request(port, "/ready")

    assert "HTTP/1.1 503" <> _ =
             request(port, "/ws?serverId=srv_unready&role=client&v=2", websocket_headers())
  end

  test "a remote owner returns a reroute response before websocket negotiation", %{port: port} do
    {peer, peer_node} = start_peer()
    server_id = "srv_remote_#{System.unique_integer([:positive])}"
    on_exit(fn -> :peer.stop(peer) end)

    {:local, _owner, _reservation} =
      :rpc.call(peer_node, PaseoRelay.Ownership, :resolve, [server_id])

    owner = :global.whereis_name({PaseoRelay.Ownership, server_id})
    assert is_pid(owner)
    assert node(owner) == peer_node

    Application.put_env(:paseo_relay, :target_mapper, &Atom.to_string/1)
    Application.put_env(:paseo_relay, :reroute_renderer, &__MODULE__.reroute/2)

    on_exit(fn ->
      Application.delete_env(:paseo_relay, :target_mapper)
      Application.delete_env(:paseo_relay, :reroute_renderer)
    end)

    response = request(port, "/ws?serverId=#{server_id}&role=client&v=2", websocket_headers())
    assert "HTTP/1.1 409" <> _ = response
    assert response =~ "x-reroute-target: #{peer_node}"
    assert response =~ "content-length: 0"
  end

  test "metrics exposes local names and values", %{port: port} do
    before = PaseoRelay.Metrics.snapshot()
    {socket, _response} = open_websocket(port, "srv_metrics")
    :ok = :gen_tcp.send(socket, <<137, 128, 0, 0, 0, 0>>)
    await_pong(socket)
    metrics = request(port, "/metrics")

    assert metric_value(metrics, "active_websockets") == before.active_websockets + 1
    assert metric_value(metrics, "active_sessions") == before.active_sessions + 1
    assert metric_value(metrics, "reroute_responses") == before.reroute_responses
    assert metric_value(metrics, "frames_forwarded") == before.frames_forwarded
    assert metric_value(metrics, "bytes_forwarded") == before.bytes_forwarded
    :gen_tcp.close(socket)
  end

  defp request(port, path, headers \\ []) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    request = [
      "GET ",
      path,
      " HTTP/1.1\r\nHost: relay.test\r\n",
      headers,
      "Connection: close\r\n\r\n"
    ]

    :ok = :gen_tcp.send(socket, request)
    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    :gen_tcp.close(socket)
    response
  end

  defp websocket_headers do
    [
      "Upgrade: websocket\r\n",
      "Connection: Upgrade\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    ]
  end

  defp open_websocket(port, server_id) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    request = [
      "GET /ws?serverId=",
      server_id,
      "&role=server&v=2 HTTP/1.1\r\nHost: relay.test\r\n",
      websocket_headers(),
      "\r\n"
    ]

    :ok = :gen_tcp.send(socket, request)
    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    {socket, response}
  end

  defp metric_value(metrics, name) do
    [_, value] = Regex.run(~r/paseo_relay_#{name} (\d+)/, metrics)
    String.to_integer(value)
  end

  def reroute(conn, target) do
    conn
    |> Plug.Conn.put_resp_header("x-reroute-target", target)
    |> Plug.Conn.send_resp(409, "")
  end

  defp start_peer do
    {:ok, peer, peer_node} =
      :peer.start_link(%{
        name: :"relay_peer_#{System.unique_integer([:positive])}",
        cookie: Node.get_cookie()
      })

    :ok = :rpc.call(peer_node, :code, :add_paths, [:code.get_path()])
    assert {:ok, _apps} = :rpc.call(peer_node, :application, :ensure_all_started, [:paseo_relay])
    {peer, peer_node}
  end

  defp await_pong(socket) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, <<138, 0>>} -> :ok
      {:ok, _frame} -> await_pong(socket)
    end
  end
end
