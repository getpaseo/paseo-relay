defmodule PaseoRelay.RouterIntegrationTest do
  use ExUnit.Case, async: false

  setup do
    assert PaseoRelay.Metrics.value(:active_websockets) == 0

    on_exit(fn ->
      assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end)
    end)

    reference = {:router_integration, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 4,
       max_connections: 1_000}
    )

    port = PaseoRelay.Listener.port(reference)
    %{port: port}
  end

  test "a locally owned websocket request upgrades", %{port: port} do
    active_websockets = PaseoRelay.Metrics.value(:active_websockets)
    {socket, response} = open_websocket(port, "srv_local")
    assert "HTTP/1.1 101" <> _ = response
    close_websocket(socket, active_websockets)
  end

  test "a non-websocket ws request is rejected before it claims session ownership", %{port: port} do
    server_id = "srv_http_#{System.unique_integer([:positive])}"

    assert "HTTP/1.1 426" <> _ =
             request(port, "/ws?serverId=#{server_id}&role=client&v=2")

    assert :undefined == PaseoRelay.Ownership.owner_pid(server_id)
  end

  test "an incomplete websocket handshake is rejected before it claims session ownership", %{
    port: port
  } do
    server_id = "srv_incomplete_#{System.unique_integer([:positive])}"

    response =
      request(
        port,
        "/ws?serverId=#{server_id}&role=client&v=2",
        ["Upgrade: websocket\r\n"]
      )

    assert :undefined == PaseoRelay.Ownership.owner_pid(server_id)
    assert "HTTP/1.1 426" <> _ = response
  end

  test "oversized route identifiers are rejected before they can claim ownership", %{port: port} do
    server_id = String.duplicate("s", 257)

    assert "HTTP/1.1 400" <> _ =
             request(port, "/ws?serverId=#{server_id}&role=client&v=2", websocket_headers())

    assert :undefined == PaseoRelay.Ownership.owner_pid(server_id)

    connection_id = String.duplicate("c", 257)

    assert "HTTP/1.1 400" <> _ =
             request(
               port,
               "/ws?serverId=srv_bounded&role=server&connectionId=#{connection_id}&v=2",
               websocket_headers()
             )
  end

  test "health is live while readiness blocks new websocket ownership" do
    visible_cluster_size =
      length(:syn.subcluster_nodes(:registry, :paseo_relay_owners)) + 1

    config = %{
      PaseoRelay.Config.defaults()
      | minimum_cluster_size: visible_cluster_size + 1
    }

    port = start_listener(config)

    assert "HTTP/1.1 200" <> _ = request(port, "/health")
    assert "HTTP/1.1 503" <> _ = request(port, "/ready")

    assert "HTTP/1.1 503" <> _ =
             request(port, "/ws?serverId=srv_unready&role=client&v=2", websocket_headers())
  end

  test "a remote owner returns a reroute response before websocket negotiation", %{port: port} do
    {peer, peer_node} = start_peer()
    server_id = "srv_remote_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end
    end)

    {:local, _owner, _reservation} =
      :rpc.call(peer_node, PaseoRelay.Ownership, :route, [server_id, "peer-target"])

    owner = await_owner(server_id)
    assert is_pid(owner)
    assert node(owner) == peer_node

    response = request(port, "/ws?serverId=#{server_id}&role=client&v=2", websocket_headers())
    assert "HTTP/1.1 409" <> _ = response
    assert response =~ "x-reroute-target: peer-target"
    assert response =~ "content-length: 0"
  end

  test "local admission pressure cannot suppress a remote owner reroute" do
    {peer, peer_node} = start_peer()
    server_id = "srv_remote_at_capacity_#{System.unique_integer([:positive])}"
    port = start_listener(PaseoRelay.Config.defaults(), max_websockets: 1)
    {local_socket, "HTTP/1.1 101" <> _response} = open_websocket(port, "local-capacity-holder")

    on_exit(fn ->
      :gen_tcp.close(local_socket)

      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end
    end)

    {:local, _owner, _reservation} =
      :rpc.call(peer_node, PaseoRelay.Ownership, :route, [server_id, "peer-target"])

    owner = await_owner(server_id)
    assert node(owner) == peer_node

    response = request(port, "/ws?serverId=#{server_id}&role=client&v=2", websocket_headers())
    assert "HTTP/1.1 409" <> _ = response
    assert response =~ "x-reroute-target: peer-target"
  end

  test "metrics exposes local names and values", %{port: port} do
    before = PaseoRelay.Metrics.snapshot()
    {socket, response} = open_websocket(port, "srv_metrics")

    # The initial sync frame arrives with the upgrade response or after it.
    <<0x81, sync_bytes, _sync_payload::binary-size(sync_bytes)>> =
      case String.split(response, "\r\n\r\n", parts: 2) do
        [_headers, ""] -> socket |> :gen_tcp.recv(0, 2_000) |> elem(1)
        [_headers, frame] -> frame
      end

    metrics = request(port, "/metrics")

    assert metric_value(metrics, "active_websockets") == before.active_websockets + 1
    assert metric_value(metrics, "active_sessions") == before.active_sessions + 1
    assert metric_value(metrics, "reroute_responses_total") == before.reroute_responses
    assert metric_value(metrics, "frames_forwarded_total") == before.frames_forwarded + 1
    assert metric_value(metrics, "bytes_forwarded_total") == before.bytes_forwarded + sync_bytes
    close_websocket(socket, before.active_websockets)
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

  defp close_websocket(socket, active_websockets) do
    mask = :crypto.strong_rand_bytes(4)
    :ok = :gen_tcp.send(socket, <<0x88, 0x80, mask::binary>>)

    assert_eventually(fn ->
      PaseoRelay.Metrics.value(:active_websockets) == active_websockets
    end)

    :ok = :gen_tcp.close(socket)
  end

  defp assert_eventually(check, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 5_000

    if check.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true")
      end

      Process.sleep(10)
      assert_eventually(check, deadline)
    end
  end

  defp metric_value(metrics, name) do
    [_, value] = Regex.run(~r/paseo_relay_#{name} (\d+)/, metrics)
    String.to_integer(value)
  end

  defp start_peer do
    {:ok, peer, peer_node} =
      :peer.start_link(%{
        name: :"relay_peer_#{System.unique_integer([:positive])}",
        args: [~c"-setcookie", Node.get_cookie() |> Atom.to_charlist()]
      })

    :ok = :rpc.call(peer_node, :code, :add_paths, [:code.get_path()])

    :ok = :rpc.call(peer_node, :application, :set_env, [:syn, :strict_mode, true])
    {:ok, _apps} = :rpc.call(peer_node, :application, :ensure_all_started, [:syn])
    :ok = :rpc.call(peer_node, :syn, :add_node_to_scopes, [[:paseo_relay_owners]])

    config = %{PaseoRelay.Config.defaults() | port: 0}

    :ok = :rpc.call(peer_node, :application, :set_env, [:paseo_relay, :runtime, config])

    assert {:ok, _apps} = :rpc.call(peer_node, :application, :ensure_all_started, [:paseo_relay])
    {peer, peer_node}
  end

  defp await_owner(server_id) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_owner(server_id, deadline)
  end

  defp await_owner(server_id, deadline) do
    case PaseoRelay.Ownership.owner_pid(server_id) do
      owner when is_pid(owner) ->
        owner

      :undefined ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("remote owner did not reach the local Syn registry")
        end

        Process.sleep(10)
        await_owner(server_id, deadline)
    end
  end

  defp start_listener(config, options \\ []) do
    reference = {:router_integration_configured, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       [
         ref: reference,
         config: config,
         ip: {127, 0, 0, 1},
         port: 0,
         acceptors: 4,
         max_connections: 1_000
       ] ++ options}
    )

    PaseoRelay.Listener.port(reference)
  end
end
