defmodule PaseoRelay.Placement.Server do
  @moduledoc false

  # Owns the home map replica, tracks peer readiness and heals sessions.
  #
  # Every tick (one second) the server:
  #   1. asks every relay peer whether it is ready, without blocking;
  #   2. when healing is enabled and this node is not draining, stops up to
  #      `heal_rate_per_sec` local sessions whose home peer has been ready for
  #      `heal_delay_ms`, idle sessions first;
  #   3. forgets old heal attempts and idle home entries.
  #
  # Heal attempts and the boomerang rule live in `Attempts`.

  use GenServer

  alias PaseoRelay.Ownership.Owner
  alias PaseoRelay.Placement.Attempts
  alias PaseoRelay.Placement.Homes
  alias PaseoRelay.Placement.Sessions

  @homes_table :paseo_relay_homes
  @scope :paseo_relay_owners
  @tick_ms 1_000
  @readiness_timeout_ms 1_000
  @idle_evictions_per_tick 1_000
  @sync_chunk_entries 5_000
  @settings [:heal_enabled, :heal_delay_ms, :heal_rate_per_sec]
  @reload_timeout_ms 2_000

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  def homes_table, do: @homes_table

  def daemon_arrived(server_id), do: GenServer.cast(__MODULE__, {:daemon_arrived, server_id})

  def settings, do: call(:settings, 5_000)

  def reload_settings, do: call(:reload_settings, @reload_timeout_ms)

  def request_reload, do: GenServer.cast(__MODULE__, :reload_settings)

  defp call(message, timeout) do
    GenServer.call(__MODULE__, message, timeout)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  @impl true
  def init(config) do
    config = PaseoRelay.RuntimeConfig.overlay(config)
    :ok = :net_kernel.monitor_nodes(true)
    homes = Homes.new(@homes_table, max_idle_ms: config.home_max_idle_ms)

    for peer <- Node.list(), do: send({__MODULE__, peer}, {:home_sync_request, node()})
    schedule_tick()

    {:ok,
     %{
       homes: homes,
       cluster_nodes: config.cluster_nodes,
       minimum_cluster_size: config.minimum_cluster_size,
       heal_enabled: config.heal_enabled,
       heal_delay_ms: config.heal_delay_ms,
       heal_rate_per_sec: config.heal_rate_per_sec,
       ready_since: %{},
       readiness_round: 0,
       applied_round: 0,
       attempts: Attempts.new(config.heal_attempt_window_ms, config.heal_cooldown_ms)
     }}
  end

  @impl true
  def handle_call(:settings, _from, state), do: {:reply, settings_of(state), state}

  def handle_call(:reload_settings, _from, state), do: {:reply, :ok, reload(state)}

  @impl true
  def handle_cast({:daemon_arrived, server_id}, state) do
    {:noreply, learn_home(state, server_id, now())}
  end

  def handle_cast(:reload_settings, state), do: {:noreply, reload(state)}

  @impl true
  def handle_info(:tick, state) do
    schedule_tick()
    now = now()

    state =
      state
      |> poll_readiness()
      |> heal(now)
      |> forget_old_attempts(now)

    Homes.evict_idle(
      state.homes,
      System.system_time(:millisecond),
      @idle_evictions_per_tick,
      &owned_on?/2
    )

    {:noreply, state}
  end

  def handle_info({:readiness, round, results}, state) when round > state.applied_round do
    connected = Node.list()
    now = now()

    ready_since =
      for {peer, {:ok, true}} <- results, peer in connected, into: %{} do
        {peer, Map.get(state.ready_since, peer, now)}
      end

    {:noreply, %{state | ready_since: ready_since, applied_round: round}}
  end

  def handle_info({:readiness, _stale_round, _results}, state), do: {:noreply, state}

  def handle_info({:home_set, server_id, home, updated_at_ms}, state) do
    Homes.put(state.homes, server_id, home, updated_at_ms)
    {:noreply, state}
  end

  def handle_info({:home_sync, entries}, state) do
    Enum.each(entries, fn {server_id, home, updated_at_ms} ->
      Homes.put(state.homes, server_id, home, updated_at_ms)
    end)

    {:noreply, state}
  end

  def handle_info({:home_sync_request, peer}, state) do
    send_snapshot(state, peer)
    {:noreply, state}
  end

  def handle_info({:nodeup, peer}, state) do
    send_snapshot(state, peer)
    {:noreply, state}
  end

  def handle_info({:nodedown, peer}, state) do
    {:noreply, %{state | ready_since: Map.delete(state.ready_since, peer)}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # A direct daemon arrival is the only evidence of a session's home.
  defp learn_home(state, server_id, now) do
    case Homes.home(@homes_table, server_id) do
      nil ->
        if cluster_connected?(state), do: set_home(state, server_id), else: state

      home when home == node() ->
        state |> set_home(server_id) |> forget_attempt(server_id)

      home ->
        count_boomerang(state, server_id, home, now)
    end
  end

  defp count_boomerang(state, server_id, home, now) do
    if Map.has_key?(state.ready_since, home) do
      case Attempts.returned(state.attempts, server_id, now) do
        {:promote, attempts} ->
          PaseoRelay.Metrics.inc(:home_promotions)
          set_home(%{state | attempts: attempts}, server_id)

        {_cooldown_or_ignored, attempts} ->
          %{state | attempts: attempts}
      end
    else
      state
    end
  end

  defp set_home(state, server_id) do
    updated_at_ms = System.system_time(:millisecond)
    Homes.put(state.homes, server_id, node(), updated_at_ms)
    message = {:home_set, server_id, node(), updated_at_ms}
    for peer <- Node.list(), do: send({__MODULE__, peer}, message)
    state
  end

  defp forget_attempt(state, server_id),
    do: %{state | attempts: Attempts.forget(state.attempts, server_id)}

  # Without the full cluster, a daemon that arrives here may have failed over
  # from its unreachable home. Learning a first home here would pin it to this node.
  defp cluster_connected?(%{cluster_nodes: []} = state),
    do: PaseoRelay.Ownership.ready?(state.minimum_cluster_size)

  defp cluster_connected?(%{cluster_nodes: cluster_nodes}) do
    connected = [node() | Node.list()]
    Enum.all?(cluster_nodes, &(&1 in connected))
  end

  # A daemon can stay connected for longer than the idle limit. Its home stays
  # while its session is owned on that home.
  defp owned_on?(server_id, home) do
    case PaseoRelay.Ownership.owner_pid(server_id) do
      owner when is_pid(owner) -> node(owner) == home
      :undefined -> false
    end
  end

  defp poll_readiness(state) do
    round = state.readiness_round + 1
    server = self()
    peers = :syn.subcluster_nodes(:registry, @scope)

    spawn(fn ->
      results = :erpc.multicall(peers, PaseoRelay.Operations, :ready?, [], @readiness_timeout_ms)
      send(server, {:readiness, round, Enum.zip(peers, results)})
    end)

    %{state | readiness_round: round}
  end

  defp heal(%{heal_enabled: false} = state, _now), do: state

  defp heal(state, now) do
    targets = healing_targets(state, now)

    if draining?() or targets == [] do
      state
    else
      state
      |> heal_candidates(targets, now)
      |> Enum.take(state.heal_rate_per_sec)
      |> Enum.reduce(state, fn {server_id, owner}, state ->
        relocate(state, server_id, owner, now)
      end)
    end
  end

  defp healing_targets(state, now) do
    for {peer, ready_since} <- state.ready_since,
        now - ready_since >= state.heal_delay_ms,
        do: peer
  end

  defp heal_candidates(state, targets, now) do
    {idle, active} =
      Sessions.fold({[], []}, fn {server_id, owner, clients}, {idle, active} ->
        cond do
          Homes.home(@homes_table, server_id) not in targets -> {idle, active}
          not Attempts.may_heal?(state.attempts, server_id, now) -> {idle, active}
          clients == 0 -> {[{server_id, owner} | idle], active}
          true -> {idle, [{server_id, owner} | active]}
        end
      end)

    idle ++ active
  end

  defp relocate(state, server_id, owner, now) do
    Owner.relocate(owner)
    PaseoRelay.Metrics.inc(:sessions_healed)
    %{state | attempts: Attempts.healed(state.attempts, server_id, now)}
  end

  defp forget_old_attempts(state, now),
    do: %{state | attempts: Attempts.prune(state.attempts, now)}

  defp send_snapshot(state, peer) do
    state.homes
    |> Homes.entries()
    |> Enum.chunk_every(@sync_chunk_entries)
    |> Enum.each(&send({__MODULE__, peer}, {:home_sync, &1}))
  end

  defp draining? do
    if Process.whereis(PaseoRelay.Drain), do: PaseoRelay.Drain.draining?(), else: false
  end

  defp settings_of(state), do: Map.take(state, @settings)

  # Reads the stored settings when the reload is handled, not when it was sent.
  defp reload(state), do: Map.merge(state, Map.take(PaseoRelay.RuntimeConfig.get(), @settings))

  defp now, do: System.monotonic_time(:millisecond)

  defp schedule_tick, do: Process.send_after(self(), :tick, @tick_ms)
end
