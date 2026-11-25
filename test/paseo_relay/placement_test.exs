defmodule PaseoRelay.PlacementTest do
  use ExUnit.Case, async: false

  alias PaseoRelay.Ownership
  alias PaseoRelay.PartitionClient
  alias PaseoRelay.Placement

  @moduletag timeout: 120_000
  @rerouted ["x-paseo-rerouted": "1"]
  @moved {:remote, 1012, "Session owner moved"}

  setup do
    suffix = System.unique_integer([:positive])
    [host] = node() |> Atom.to_string() |> String.split("@") |> tl()
    a = :"heal_a_#{suffix}@#{host}"
    b = :"heal_b_#{suffix}@#{host}"
    %{cluster: %{a: a, b: b, nodes: [a, b], peers: %{}, ports: %{}}}
  end

  test "a session that failed over heals back once its home is ready again", %{cluster: cluster} do
    cluster =
      cluster |> start_relay(:a, heal_delay_ms: 1_500) |> start_relay(:b, heal_delay_ms: 1_500)

    id = server_id("failover")

    {:ok, first} = daemon(cluster, :a, id)
    assert await_home(cluster, :b, id) == cluster.a

    cluster = stop_relay(cluster, :a)
    assert_receive {:partition_closed, ^first, _reason}, 5_000
    second = await_daemon(cluster, :b, id)
    assert owner_node(cluster, :b, id) == cluster.b

    # Without its full cluster, B does not pin a new daemon's first home to itself.
    unseen = server_id("unseen")
    _unseen_daemon = await_daemon(cluster, :b, unseen)
    barrier(cluster, :b)
    assert rpc(cluster, :b, Placement, :home, [unseen]) == nil
    assert metric(cluster, :b, "misplaced_sessions") == 1

    cluster = start_relay(cluster, :a, heal_delay_ms: 1_500)
    returned_at = System.monotonic_time(:millisecond)
    assert_receive {:partition_closed, ^second, @moved}, 10_000
    # B saw A's return at most moments before `start_relay` returned.
    assert System.monotonic_time(:millisecond) - returned_at >= 1_000

    # The stopped Owner released the session at once, without its idle grace.
    third = await_daemon(cluster, :a, id)
    assert owner_node(cluster, :a, id) == cluster.a
    assert metric(cluster, :b, "sessions_healed_total") == 1
    assert metric(cluster, :b, "misplaced_sessions") == 0
    refute_received {:partition_closed, ^third, _reason}
  end

  test "idle sessions heal first and each tick is rate limited", %{cluster: cluster} do
    cluster =
      cluster
      |> start_relay(:a, heal_delay_ms: 0)
      |> start_relay(:b, heal_delay_ms: 0, heal_enabled: false, heal_rate_per_sec: 2)

    idle = for index <- 1..3, do: server_id("idle_#{index}")
    active = for index <- 1..2, do: server_id("active_#{index}")

    daemons =
      Map.new(idle ++ active, fn id -> {misplace(cluster, id), id} end)

    for id <- active do
      {:ok, _client} = connect(cluster, :b, id, "client", "c1")
    end

    await_metric(cluster, :b, "misplaced_sessions", 5)

    assert {:ok, %{heal_enabled: true}} =
             rpc(cluster, :b, PaseoRelay.RuntimeConfig, :put, [%{heal_enabled: true}])

    closes =
      for _index <- 1..5 do
        assert_receive {:partition_closed, daemon, @moved} when is_map_key(daemons, daemon),
                       10_000

        {Map.fetch!(daemons, daemon), System.monotonic_time(:millisecond)}
      end

    assert [first, second, third] = tick_batches(closes)
    assert length(first) == 2 and Enum.all?(first, &(&1 in idle))
    assert Enum.sort(second) == Enum.sort((idle -- first) ++ (second -- idle))
    assert length(second) == 2 and length(second -- idle) == 1
    assert length(third) == 1 and Enum.all?(third, &(&1 in active))
    assert metric(cluster, :b, "sessions_healed_total") == 5
  end

  test "healing pauses while the home node drains, is under memory pressure or disconnects", %{
    cluster: cluster
  } do
    cluster =
      cluster
      |> start_relay(:a, heal_delay_ms: 0)
      |> start_relay(:b, heal_delay_ms: 0, heal_enabled: false)

    for {label, pause, resume} <- [
          {"drain", {PaseoRelay.Drain, :begin, []}, {PaseoRelay.Drain, :cancel, []}},
          {"pressure", {PaseoRelay.Capacity, :set_watermark, [1, 5_000]},
           {PaseoRelay.Capacity, :set_watermark, [0, 5_000]}}
        ] do
      id = server_id(label)
      daemon = misplace(cluster, id)
      {module, function, arguments} = pause
      rpc(cluster, :a, module, function, arguments)
      if label == "pressure", do: rpc(cluster, :a, PaseoRelay.Capacity, :check_now, [5_000])
      # One readiness poll sees the paused home before healing starts.
      Process.sleep(1_500)
      put_settings(cluster, :b, %{heal_enabled: true})
      refute_receive {:partition_closed, ^daemon, _reason}, 2_500
      assert owner_node(cluster, :b, id) == cluster.b

      {module, function, arguments} = resume
      rpc(cluster, :a, module, function, arguments)
      assert_receive {:partition_closed, ^daemon, @moved}, 5_000
      put_settings(cluster, :b, %{heal_enabled: false})
    end

    # A disconnect restarts the ready period: after it, B waits the full delay again.
    id = server_id("disconnect")
    daemon = misplace(cluster, id)
    put_settings(cluster, :b, %{heal_enabled: true, heal_delay_ms: 60_000})
    true = rpc(cluster, :b, :erlang, :disconnect_node, [cluster.a])
    disconnected_at = System.monotonic_time(:millisecond)
    put_settings(cluster, :b, %{heal_delay_ms: 3_000})

    assert_receive {:partition_closed, ^daemon, @moved}, 15_000
    assert System.monotonic_time(:millisecond) - disconnected_at >= 3_000
  end

  test "a daemon that returns after two heals moves its home to this node", %{cluster: cluster} do
    timing = [heal_delay_ms: 0, heal_attempt_window_ms: 2_000, heal_cooldown_ms: 3_000]
    cluster = cluster |> start_relay(:a, timing) |> start_relay(:b, timing)
    id = server_id("traveler")

    first = misplace(cluster, id)
    assert_receive {:partition_closed, ^first, @moved}, 5_000

    # The first direct return starts a cooldown before the next heal.
    second = await_daemon(cluster, :b, id)
    refute_receive {:partition_closed, ^second, _reason}, 2_000
    assert_receive {:partition_closed, ^second, @moved}, 5_000

    # The second direct return moves the home here and stops the loop.
    third = await_daemon(cluster, :b, id)
    assert await_home(cluster, :a, id, cluster.b) == cluster.b
    assert rpc(cluster, :b, Placement, :home, [id]) == cluster.b
    assert metric(cluster, :b, "home_promotions_total") == 1
    refute_receive {:partition_closed, ^third, _reason}, 3_000
    assert metric(cluster, :b, "sessions_healed_total") == 2
  end

  test "a session a client claimed away from the daemon's home heals to that home", %{
    cluster: cluster
  } do
    cluster =
      cluster
      |> start_relay(:a, heal_delay_ms: 0)
      |> start_relay(:b, heal_delay_ms: 0, heal_enabled: false)

    id = server_id("client_won")

    {:ok, client} = connect(cluster, :b, id, "client", "c1")
    barrier(cluster, :b)
    assert rpc(cluster, :b, Placement, :home, [id]) == nil

    # The daemon reaches its home after the client, gets a reroute and is
    # forwarded to the owner on B.
    await_remote_owner(cluster, :a, id)
    assert {:error, %WebSockex.RequestError{code: 409}} = daemon(cluster, :a, id)
    {:ok, rerouted} = daemon(cluster, :b, id, @rerouted)
    assert await_home(cluster, :b, id) == cluster.a
    put_settings(cluster, :b, %{heal_enabled: true})

    assert_receive {:partition_closed, ^client, @moved}, 5_000
    assert_receive {:partition_closed, ^rerouted, @moved}, 5_000
    _home = await_daemon(cluster, :a, id)
    assert owner_node(cluster, :a, id) == cluster.a
  end

  # Gives B a session whose daemon's home is A: the daemon is forwarded to B,
  # then a direct arrival at A records the home.
  test "a daemon that stays connected keeps its home past the idle limit", %{cluster: cluster} do
    timing = [home_max_idle_ms: 2_000]
    cluster = cluster |> start_relay(:a, timing) |> start_relay(:b, timing)
    id = server_id("long_lived")
    ended = server_id("ended")

    {:ok, connected} = daemon(cluster, :a, id)
    assert await_home(cluster, :b, id) == cluster.a

    # A replicated entry for a session that no longer exists anywhere.
    for relay <- [:a, :b] do
      send(
        {Placement.Server, Map.fetch!(cluster, relay)},
        {:home_set, ended, cluster.a, System.system_time(:millisecond)}
      )

      assert await_home(cluster, relay, ended) == cluster.a
    end

    Process.sleep(4_500)

    for relay <- [:a, :b] do
      assert rpc(cluster, relay, Placement, :home, [id]) == cluster.a
      assert rpc(cluster, relay, Placement, :home, [ended]) == nil
    end

    refute_received {:partition_closed, ^connected, _reason}
  end

  defp misplace(cluster, id) do
    {:ok, daemon} = daemon(cluster, :b, id, @rerouted)
    await_remote_owner(cluster, :a, id)
    assert {:error, %WebSockex.RequestError{code: 409}} = daemon(cluster, :a, id)
    assert await_home(cluster, :b, id) == cluster.a
    daemon
  end

  # Groups closes that arrived within one heal tick, keeping only session ids.
  defp tick_batches(closes) do
    closes
    |> Enum.chunk_while(
      [],
      fn
        close, [] ->
          {:cont, [close]}

        {_id, at} = close, [{_, previous} | _] = batch when at - previous < 500 ->
          {:cont, [close | batch]}

        close, batch ->
          {:cont, Enum.reverse(batch), [close]}
      end,
      fn batch -> {:cont, Enum.reverse(batch), []} end
    )
    |> Enum.map(fn batch -> Enum.map(batch, &elem(&1, 0)) end)
  end

  defp server_id(label), do: "srv_heal_#{label}_#{System.unique_integer([:positive])}"

  defp daemon(cluster, relay, id, headers \\ []),
    do: connect(cluster, relay, id, "server", "", headers)

  defp connect(cluster, relay, id, role, connection_id, headers \\ []) do
    port = Map.fetch!(cluster.ports, relay)

    url =
      "ws://127.0.0.1:#{port}/ws?serverId=#{id}&role=#{role}&v=2&connectionId=#{connection_id}"

    PartitionClient.connect(url, self(), headers)
  end

  # A daemon retries until the relay admits it, as during a real reconnect.
  defp await_daemon(cluster, relay, id, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 10_000

    case daemon(cluster, relay, id) do
      {:ok, daemon} ->
        daemon

      {:error, reason} ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: flunk("daemon was not admitted: #{inspect(reason)}")

        Process.sleep(50)
        await_daemon(cluster, relay, id, deadline)
    end
  end

  defp await_home(cluster, relay, id, expected \\ nil, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 5_000
    home = rpc(cluster, relay, Placement, :home, [id])

    cond do
      home != nil and (expected == nil or home == expected) ->
        home

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("#{relay} did not learn the home of #{id}: #{inspect(home)}")

      true ->
        Process.sleep(20)
        await_home(cluster, relay, id, expected, deadline)
    end
  end

  defp await_remote_owner(cluster, relay, id, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 5_000

    case rpc(cluster, relay, Ownership, :resolve, [id]) do
      {:reroute, _target} ->
        :ok

      other ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: flunk("#{relay} did not see the remote owner of #{id}: #{inspect(other)}")

        Process.sleep(20)
        await_remote_owner(cluster, relay, id, deadline)
    end
  end

  defp await_metric(cluster, relay, name, expected, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 5_000
    value = metric(cluster, relay, name)

    cond do
      value == expected -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("#{name} stayed #{value}")
      true -> await_metric_after_sleep(cluster, relay, name, expected, deadline)
    end
  end

  defp await_metric_after_sleep(cluster, relay, name, expected, deadline) do
    Process.sleep(20)
    await_metric(cluster, relay, name, expected, deadline)
  end

  # A call returns after Placement handled every arrival recorded before it.
  defp barrier(cluster, relay), do: rpc(cluster, relay, Placement, :settings, [])

  defp owner_node(cluster, relay, id) do
    case rpc(cluster, relay, Ownership, :owner_pid, [id]) do
      owner when is_pid(owner) -> node(owner)
      :undefined -> nil
    end
  end

  defp metric(cluster, relay, name) do
    {200, _content_type, body} =
      rpc(cluster, relay, PaseoRelay.Operations, :response, ["/metrics"])

    [_, value] = Regex.run(~r/^paseo_relay_#{name} (\d+)$/m, body)
    String.to_integer(value)
  end

  defp put_settings(cluster, relay, settings),
    do: {:ok, _settings} = rpc(cluster, relay, PaseoRelay.RuntimeConfig, :put, [settings])

  defp rpc(cluster, relay, module, function, arguments),
    do: :erpc.call(Map.fetch!(cluster, relay), module, function, arguments, 10_000)

  defp start_relay(cluster, relay, settings) do
    name = cluster |> Map.fetch!(relay) |> Atom.to_string() |> String.split("@") |> hd()

    # The control channel is not distribution, so a test that disconnects two
    # relays cannot stop a peer. Without overlapping-partition prevention, the
    # disconnect also stays between those two relays.
    {:ok, peer, peer_node} =
      :peer.start(%{
        name: String.to_atom(name),
        connection: 0,
        args: [
          ~c"-setcookie",
          Atom.to_charlist(Node.get_cookie()),
          ~c"-kernel",
          ~c"prevent_overlapping_partitions",
          ~c"false",
          ~c"-kernel",
          ~c"logger_level",
          ~c"warning",
          ~c"-pa" | :code.get_path()
        ]
      })

    on_exit(fn -> stop_peer(peer) end)
    true = Node.connect(peer_node)

    :ok = :erpc.call(peer_node, :application, :set_env, [:syn, :strict_mode, true])
    {:ok, _apps} = :erpc.call(peer_node, :application, :ensure_all_started, [:syn])
    :ok = :erpc.call(peer_node, :syn, :add_node_to_scopes, [[:paseo_relay_owners]])

    config =
      struct!(
        %{
          PaseoRelay.Config.defaults()
          | port: 0,
            ownership_target: Atom.to_string(peer_node),
            cluster_nodes: cluster.nodes
        },
        settings
      )

    :ok = :erpc.call(peer_node, :application, :set_env, [:paseo_relay, :runtime, config])
    {:ok, _apps} = :erpc.call(peer_node, :application, :ensure_all_started, [:paseo_relay])
    port = :erpc.call(peer_node, :ranch, :get_port, [PaseoRelay.Listener])

    cluster = %{
      cluster
      | peers: Map.put(cluster.peers, relay, peer),
        ports: Map.put(cluster.ports, relay, port)
    }

    await_cluster(cluster)
    cluster
  end

  defp stop_relay(cluster, relay) do
    stop_peer(Map.fetch!(cluster.peers, relay))
    %{cluster | peers: Map.delete(cluster.peers, relay), ports: Map.delete(cluster.ports, relay)}
  end

  defp stop_peer(peer) do
    :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end

  # Every running relay sees every other running relay in the Syn scope.
  defp await_cluster(cluster, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 10_000
    running = Enum.map(Map.keys(cluster.peers), &Map.fetch!(cluster, &1))

    converged =
      Enum.all?(running, fn relay ->
        visible = :erpc.call(relay, :syn, :subcluster_nodes, [:registry, :paseo_relay_owners])
        Enum.all?(running -- [relay], &(&1 in visible))
      end)

    cond do
      converged -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("relay cluster did not converge")
      true -> await_cluster_after_sleep(cluster, deadline)
    end
  end

  defp await_cluster_after_sleep(cluster, deadline) do
    Process.sleep(20)
    await_cluster(cluster, deadline)
  end
end
