defmodule PaseoRelay.OwnershipTest do
  use ExUnit.Case

  alias PaseoRelay.Ownership
  alias PaseoRelay.Ownership.Owner

  test "claims an unowned server locally and keeps later local requests local" do
    assert :local = Ownership.claim("server-a", "opaque-owner-a")
    assert :local = Ownership.resolve("server-a")
  end

  test "clears ownership when the owner dies so another request can claim it" do
    parent = self()

    owner =
      spawn(fn ->
        assert :local = Ownership.claim("server-b", "opaque-owner-b")
        send(parent, :claimed)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :claimed
    owner_record = Ownership.owner_pid("server-b")
    owner_down = Process.monitor(owner_record)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_down, :process, ^owner_record, :normal}

    assert :unowned = Ownership.resolve("server-b")
    assert :local = Ownership.claim("server-b", "opaque-owner-c")
  end

  test "tolerates brief scheduler pressure while reserving a live owner" do
    server_id = "pressured-owner-#{System.unique_integer([:positive])}"
    {:ok, owner} = Owner.start(server_id, "local")
    :ok = :sys.suspend(owner)

    reservation = Task.async(fn -> Owner.reserve(owner) end)
    Process.sleep(1_100)

    assert nil == Task.yield(reservation, 0)
    :ok = :sys.resume(owner)
    assert {:ok, _token} = Task.await(reservation, 1_000)

    Process.exit(owner, :kill)
  end
end

defmodule PaseoRelay.RerouteTest do
  use ExUnit.Case, async: true

  test "renders an opaque reroute target into a configured response header" do
    assert %{"x-reroute-target" => "machine-opaque-id"} =
             PaseoRelay.Reroute.headers({:reroute, "machine-opaque-id"}, "x-reroute-target")

    assert %{} = PaseoRelay.Reroute.headers(:local, "x-reroute-target")
  end
end

defmodule PaseoRelay.DistributedOwnershipTest do
  use ExUnit.Case

  alias PaseoRelay.Ownership
  alias PaseoRelay.PartitionClient

  @surge_count String.to_integer(System.get_env("PASEO_OWNERSHIP_SURGE_COUNT", "1000"))

  setup_all do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])

      {:ok, _} =
        Node.start(
          String.to_atom("relay_test_" <> Integer.to_string(System.unique_integer([:positive]))),
          :shortnames
        )
    end

    :erlang.set_cookie(node(), :relay_test_cookie)

    {:ok, peer_a, peer_node_a} =
      :peer.start_link(%{
        name: :relay_peer_a,
        connection: 0,
        args: [
          ~c"-setcookie",
          ~c"relay_test_cookie",
          ~c"-connect_all",
          ~c"false",
          ~c"-kernel",
          ~c"dist_auto_connect",
          ~c"never",
          ~c"-pa"
          | :code.get_path()
        ]
      })

    {:ok, peer_b, peer_node_b} =
      :peer.start_link(%{
        name: :relay_peer_b,
        connection: 0,
        args: [
          ~c"-setcookie",
          ~c"relay_test_cookie",
          ~c"-connect_all",
          ~c"false",
          ~c"-kernel",
          ~c"dist_auto_connect",
          ~c"never",
          ~c"-pa"
          | :code.get_path()
        ]
      })

    start_syn(peer_node_a)
    start_syn(peer_node_b)
    assert Node.connect(peer_node_a)
    assert :rpc.call(peer_node_a, Node, :connect, [peer_node_b])
    await_syn_cluster([node(), peer_node_a, peer_node_b])
    port_a = start_relay(peer_node_a)
    port_b = start_relay(peer_node_b)

    on_exit(fn ->
      stop_peer(peer_a)
      stop_peer(peer_b)
    end)

    %{
      peer: peer_node_a,
      peers: [peer_node_a, peer_node_b],
      peer_ports: %{peer_node_a => port_a, peer_node_b => port_b}
    }
  end

  test "concurrent claims choose one owner and remote requests receive its opaque target", %{
    peer: peer
  } do
    results =
      Task.await_many([
        Task.async(fn -> Ownership.route("server-c", "opaque-owner-a") end),
        Task.async(fn ->
          :rpc.call(peer, Ownership, :route, ["server-c", "opaque-owner-b"])
        end)
      ])

    owner_pids = local_owner_pids(results)
    winner = await_owner_consensus("server-c", peer)

    assert length(owner_pids) in 1..2
    assert Enum.count(owner_pids, &process_alive?/1) == 1
    assert winner in owner_pids

    assert Enum.any?(
             [Ownership.resolve("server-c"), :rpc.call(peer, Ownership, :resolve, ["server-c"])],
             &(&1 == :local)
           )

    assert Enum.any?(
             [Ownership.resolve("server-c"), :rpc.call(peer, Ownership, :resolve, ["server-c"])],
             &match?(
               {:reroute, winner_target}
               when winner_target in ["opaque-owner-a", "opaque-owner-b"],
               &1
             )
           )
  end

  test "a remote lookup returns the local winner's advertised opaque target", %{peer: peer} do
    assert :local = Ownership.claim("server-d", "opaque-owner-a")

    assert {:reroute, "opaque-owner-a"} =
             await_resolve(peer, "server-d", {:reroute, "opaque-owner-a"})
  end

  test "a remote node can claim after the current owner dies", %{peer: peer} do
    parent = self()

    local_session =
      spawn(fn ->
        send(parent, {:claimed, Ownership.claim("server-e", "opaque-owner-a")})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:claimed, :local}, 5_500
    owner_record = Ownership.owner_pid("server-e")
    owner_down = Process.monitor(owner_record)
    Process.exit(local_session, :kill)
    assert_receive {:DOWN, ^owner_down, :process, ^owner_record, :normal}, 5_500

    remote_session = :rpc.call(peer, :erlang, :spawn, [:timer, :sleep, [:infinity]])

    assert :local =
             :rpc.call(peer, Ownership, :claim, ["server-e", "opaque-owner-b", remote_session])

    assert {:reroute, "opaque-owner-b"} = Ownership.resolve("server-e")
  end

  @tag timeout: 30_000
  test "partition healing keeps one real websocket owner and reroutes the loser", %{
    peers: [peer_a, peer_b],
    peer_ports: ports
  } do
    server_id = "partition-#{System.unique_integer([:positive])}"

    assert true = :rpc.call(peer_a, :erlang, :disconnect_node, [peer_b])
    await_partition(peer_a, peer_b)

    {:ok, client_a} = connect_on(peer_a, Map.fetch!(ports, peer_a), server_id)
    assert_receive {:partition_open, ^client_a}

    {:ok, client_b} = connect_on(peer_b, Map.fetch!(ports, peer_b), server_id)
    assert_receive {:partition_open, ^client_b}

    assert true = :rpc.call(peer_a, :net_kernel, :connect_node, [peer_b])
    assert true = :rpc.call(peer_b, :net_kernel, :connect_node, [peer_a])

    winner = await_owner_consensus(server_id, [node(), peer_a, peer_b])

    {winner_client, loser_client, loser_node} =
      clients_by_winner(winner, peer_a, client_a, peer_b, client_b)

    assert_receive {:partition_closed, ^loser_client, {:remote, 1012, "Session owner moved"}},
                   5_000

    refute_receive {:partition_closed, ^winner_client, _reason}, 250

    assert {:reroute, target} = :rpc.call(loser_node, Ownership, :resolve, [server_id])
    assert target == Atom.to_string(node(winner))

    response = websocket_upgrade(Map.fetch!(ports, loser_node), server_id)
    assert "HTTP/1.1 409" <> _ = response
    assert response =~ "x-reroute-target: #{target}"

    Process.exit(winner_client, :kill)
  end

  @tag timeout: 120_000
  test "distinct servers claim owners across three nodes during a reconnect surge", %{
    peers: [peer_a, peer_b]
  } do
    prefix = "surge-#{System.unique_integer([:positive])}"
    observers = [node(), peer_a, peer_b]
    baseline = registry_counts(observers)

    entries =
      [:local, peer_a, peer_b]
      |> Stream.cycle()
      |> Stream.take(@surge_count)
      |> Stream.with_index(1)
      |> Enum.map(fn {landing, index} -> {landing, "#{prefix}-#{index}"} end)

    results =
      entries
      |> Task.async_stream(
        fn {landing, server_id} -> route_on(landing, server_id) end,
        max_concurrency: 512,
        timeout: 20_000,
        ordered: false
      )
      |> Enum.to_list()

    assert length(results) == @surge_count
    assert Enum.count(results, &match?({:ok, {:local, _, _}}, &1)) == @surge_count

    expected_counts = expected_registry_counts(baseline, entries)
    assert expected_counts == await_registry_counts(observers, expected_counts)

    observer_by_landing = %{:local => peer_a, peer_a => peer_b, peer_b => node()}

    actual_routes =
      entries
      |> Enum.take(3)
      |> Enum.map(fn {landing, server_id} ->
        resolve_on(Map.fetch!(observer_by_landing, landing), server_id)
      end)

    assert actual_routes ==
             entries
             |> Enum.take(3)
             |> Enum.map(fn {landing, _server_id} -> {:reroute, target_for(landing)} end)
  end

  defp route_on(:local, server_id), do: Ownership.route(server_id, "local")

  defp route_on(peer, server_id),
    do: :rpc.call(peer, Ownership, :route, [server_id, Atom.to_string(peer)])

  defp start_relay(peer) do
    load_module(peer, PartitionClient)

    :ok =
      :rpc.call(peer, :application, :set_env, [
        :paseo_relay,
        :operations,
        [host: "127.0.0.1", ip: {127, 0, 0, 1}, port: 0, drain: false]
      ])

    :ok =
      :rpc.call(peer, :application, :set_env, [
        :paseo_relay,
        :ownership_target,
        Atom.to_string(peer)
      ])

    :ok =
      :rpc.call(peer, :application, :set_env, [
        :paseo_relay,
        :reroute_header,
        "x-reroute-target"
      ])

    {:ok, _applications} =
      :rpc.call(peer, :application, :ensure_all_started, [:paseo_relay])

    children = :rpc.call(peer, Supervisor, :which_children, [PaseoRelay.Supervisor])

    {_id, listener, :supervisor, _modules} =
      Enum.find(children, fn {_id, _pid, _type, modules} -> Bandit in modules end)

    {:ok, {_address, port}} =
      :rpc.call(peer, ThousandIsland, :listener_info, [listener])

    port
  end

  defp load_module(peer, module) do
    {^module, binary, path} = :code.get_object_code(module)
    {:module, ^module} = :rpc.call(peer, :code, :load_binary, [module, path, binary])
  end

  defp connect_on(peer, port, server_id) do
    url = "ws://127.0.0.1:#{port}/ws?serverId=#{server_id}&role=server&v=2"
    :rpc.call(peer, PartitionClient, :start, [url, self()])
  end

  defp await_partition(peer_a, peer_b) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_partition(peer_a, peer_b, deadline)
  end

  defp await_partition(peer_a, peer_b, deadline) do
    separated =
      peer_b not in :rpc.call(peer_a, Node, :list, []) and
        peer_a not in :rpc.call(peer_b, Node, :list, [])

    if separated do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("peer nodes did not partition")
      end

      Process.sleep(10)
      await_partition(peer_a, peer_b, deadline)
    end
  end

  defp clients_by_winner(winner, peer_a, client_a, peer_b, client_b) do
    case node(winner) do
      ^peer_a -> {client_a, client_b, peer_b}
      ^peer_b -> {client_b, client_a, peer_a}
      other -> flunk("unexpected partition winner on #{other}")
    end
  end

  defp websocket_upgrade(port, server_id) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    request =
      "GET /ws?serverId=#{server_id}&role=client&v=2 HTTP/1.1\r\n" <>
        "Host: relay.test\r\n" <>
        "Upgrade: websocket\r\n" <>
        "Connection: Upgrade\r\n" <>
        "Sec-WebSocket-Version: 13\r\n" <>
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"

    :ok = :gen_tcp.send(socket, request)
    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    :gen_tcp.close(socket)
    response
  end

  defp resolve_on(observer, server_id) do
    if observer == node() do
      Ownership.resolve(server_id)
    else
      :rpc.call(observer, Ownership, :resolve, [server_id])
    end
  end

  defp target_for(:local), do: "local"
  defp target_for(peer), do: Atom.to_string(peer)

  defp expected_registry_counts(baseline, entries) do
    increments =
      entries
      |> Enum.map(fn {landing, _server_id} -> landing_node(landing) end)
      |> Enum.frequencies()

    Map.new(baseline, fn {{observer, origin}, count} ->
      {{observer, origin}, count + Map.get(increments, origin, 0)}
    end)
  end

  defp landing_node(:local), do: node()
  defp landing_node(peer), do: peer

  defp registry_counts(nodes) do
    Map.new(
      for observer <- nodes,
          origin <- nodes,
          do: {{observer, origin}, registry_count(observer, origin)}
    )
  end

  defp registry_count(observer, origin) do
    if observer == node() do
      :syn.registry_count(:paseo_relay_owners, origin)
    else
      :rpc.call(observer, :syn, :registry_count, [:paseo_relay_owners, origin])
    end
  end

  defp await_registry_counts(observers, expected) do
    deadline = System.monotonic_time(:millisecond) + 30_000
    await_registry_counts(observers, expected, deadline)
  end

  defp await_registry_counts(observers, expected, deadline) do
    counts = registry_counts(observers)

    if counts == expected do
      counts
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk(
          "Syn registry did not converge: expected #{inspect(expected)}, got #{inspect(counts)}"
        )
      end

      Process.sleep(10)
      await_registry_counts(observers, expected, deadline)
    end
  end

  defp stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp await_owner_consensus(server_id, peer) when is_atom(peer) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_owner_consensus(server_id, peer, deadline)
  end

  defp await_owner_consensus(server_id, observers) when is_list(observers) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_owner_consensus_on(server_id, observers, deadline)
  end

  defp await_owner_consensus_on(server_id, observers, deadline) do
    owners =
      Enum.map(observers, fn observer ->
        if observer == node() do
          Ownership.owner_pid(server_id)
        else
          :rpc.call(observer, Ownership, :owner_pid, [server_id])
        end
      end)

    case Enum.uniq(owners) do
      [owner] when is_pid(owner) ->
        owner

      _owners ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("Syn ownership did not converge: #{inspect(owners)}")
        end

        Process.sleep(10)
        await_owner_consensus_on(server_id, observers, deadline)
    end
  end

  defp await_owner_consensus(server_id, peer, deadline) do
    owners = [Ownership.owner_pid(server_id), :rpc.call(peer, Ownership, :owner_pid, [server_id])]

    case Enum.uniq(owners) do
      [owner] when is_pid(owner) ->
        owner

      _owners ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("Syn ownership did not converge: #{inspect(owners)}")
        end

        Process.sleep(10)
        await_owner_consensus(server_id, peer, deadline)
    end
  end

  defp process_alive?(pid), do: :rpc.call(node(pid), Process, :alive?, [pid])

  defp await_resolve(peer, server_id, expected) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_resolve(peer, server_id, expected, deadline)
  end

  defp await_resolve(peer, server_id, expected, deadline) do
    case :rpc.call(peer, Ownership, :resolve, [server_id]) do
      ^expected ->
        expected

      _other ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("Syn ownership did not reach #{peer}")
        end

        Process.sleep(10)
        await_resolve(peer, server_id, expected, deadline)
    end
  end

  defp local_owner_pids(results) do
    Enum.flat_map(results, fn
      {:local, owner, _reservation} -> [owner]
      {:reroute, _target} -> []
    end)
  end

  defp start_syn(peer) do
    :ok = :rpc.call(peer, :application, :set_env, [:syn, :strict_mode, true])
    {:ok, _applications} = :rpc.call(peer, :application, :ensure_all_started, [:syn])
    :ok = :rpc.call(peer, :syn, :add_node_to_scopes, [[:paseo_relay_owners]])
  end

  defp await_syn_cluster(nodes) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_syn_cluster(nodes, deadline)
  end

  defp await_syn_cluster(nodes, deadline) do
    members =
      Enum.map(nodes, fn peer ->
        :rpc.call(peer, :syn, :subcluster_nodes, [:registry, :paseo_relay_owners])
      end)

    converged =
      nodes
      |> Enum.zip(members)
      |> Enum.all?(fn {querying_node, visible_nodes} ->
        expected_nodes = nodes |> List.delete(querying_node) |> MapSet.new()
        MapSet.subset?(expected_nodes, MapSet.new(visible_nodes))
      end)

    if converged do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("Syn cluster did not converge: #{inspect(members)}")
      end

      Process.sleep(10)
      await_syn_cluster(nodes, deadline)
    end
  end
end
