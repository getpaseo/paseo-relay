defmodule PaseoRelay.Ownership do
  @moduledoc false

  @claim_timeout 1_000

  def route(server_id, target) do
    case :global.whereis_name(name(server_id)) do
      owner when is_pid(owner) -> route_owner(owner)
      :undefined -> claim_route(server_id, target)
    end
  end

  # Kept for the existing ownership contract.
  def claim(server_id, target), do: claim(server_id, target, self())

  def claim(server_id, target, session_owner) do
    case route(server_id, target) do
      {:local, owner, reservation} ->
        :ok = Owner.attach(owner, reservation, session_owner)
        :local

      other ->
        other
    end
  end

  def resolve(server_id) do
    case :global.whereis_name(name(server_id)) do
      :undefined -> :unowned
      owner when node(owner) == node() -> :local
      owner -> reroute(owner)
    end
  end

  def ready?,
    do: length(Node.list()) + 1 >= Application.get_env(:paseo_relay, :minimum_cluster_size, 1)

  def name(server_id), do: {__MODULE__, server_id}

  defp claim_route(server_id, target) do
    cond do
      PaseoRelay.Drain.draining?() -> {:unavailable, :draining}
      not ready?() -> {:unavailable, :cluster}
      true -> transact(server_id, target)
    end
  end

  defp transact(server_id, target) do
    requester = {node(), self(), make_ref()}

    task =
      Task.async(fn ->
        :global.trans({{__MODULE__, server_id}, requester}, fn ->
          start_or_route(server_id, target)
        end)
      end)

    case Task.yield(task, @claim_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, route} -> route
      nil -> {:unavailable, :owner}
    end
  end

  defp start_or_route(server_id, target) do
    case :global.whereis_name(name(server_id)) do
      owner when is_pid(owner) ->
        route_owner(owner)

      :undefined ->
        {:ok, owner} = Owner.start(server_id, target)

        if :global.register_name(name(server_id), owner) do
          route_owner(owner)
        else
          Process.exit(owner, :shutdown)
          route_owner(:global.whereis_name(name(server_id)))
        end
    end
  end

  defp route_owner(owner) when is_pid(owner) and node(owner) == node() do
    case Owner.reserve(owner) do
      {:ok, reservation} -> {:local, owner, reservation}
      :closed -> {:unavailable, :owner}
    end
  end

  defp route_owner(owner) when is_pid(owner), do: reroute(owner)
  defp route_owner(:undefined), do: {:unavailable, :owner}

  defp reroute(owner) do
    case Owner.target(owner) do
      {:ok, target} -> {:reroute, target}
      :unavailable -> {:unavailable, :owner}
    end
  end
end

defmodule PaseoRelay.Ownership.Owner do
  use GenServer

  @reservation_ms 5_000
  @idle_ms 30_000

  def start(server_id, target), do: GenServer.start(__MODULE__, {server_id, target})
  def reserve(owner), do: GenServer.call(owner, :reserve, 1_000)

  def attach(owner, reservation, socket),
    do: GenServer.call(owner, {:attach, reservation, socket}, 1_000)

  def detach(owner, socket), do: GenServer.cast(owner, {:detach, socket})

  def target(owner) do
    try do
      GenServer.call(owner, :target, 1_000)
    catch
      :exit, _ -> :unavailable
    end
  end

  @impl true
  def init({server_id, target}) do
    PaseoRelay.Metrics.inc(:active_sessions)
    {:ok, %{server_id: server_id, target: target, reservations: %{}, sockets: %{}, idle: nil}}
  end

  @impl true
  def handle_call(:target, _from, state), do: {:reply, {:ok, state.target}, state}

  def handle_call(:reserve, _from, state) do
    token = make_ref()
    timer = Process.send_after(self(), {:expired, token}, @reservation_ms)

    {:reply, {:ok, token},
     %{cancel_idle(state) | reservations: Map.put(state.reservations, token, timer)}}
  end

  def handle_call({:attach, token, socket}, _from, state) do
    case Map.pop(state.reservations, token) do
      {nil, _} ->
        {:reply, :closed, state}

      {timer, reservations} ->
        Process.cancel_timer(timer)

        {:reply, :ok,
         %{
           cancel_idle(state)
           | reservations: reservations,
             sockets: Map.put(state.sockets, socket, Process.monitor(socket))
         }}
    end
  end

  @impl true
  def handle_cast({:detach, socket}, state), do: {:noreply, remove_socket(state, socket)}

  @impl true
  def handle_info({:expired, token}, state),
    do:
      {:noreply, state |> Map.update!(:reservations, &Map.delete(&1, token)) |> idle_when_empty()}

  def handle_info({:DOWN, ref, :process, socket, _}, state) when state.sockets[socket] == ref,
    do: {:noreply, remove_socket(state, socket)}

  def handle_info(:idle, %{reservations: reservations, sockets: sockets} = state)
      when map_size(reservations) == 0 and map_size(sockets) == 0, do: {:stop, :normal, state}

  def handle_info(:idle, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    name = PaseoRelay.Ownership.name(state.server_id)
    if :global.whereis_name(name) == self(), do: :global.unregister_name(name)
    PaseoRelay.Metrics.dec(:active_sessions)
  end

  defp remove_socket(state, socket) do
    case Map.pop(state.sockets, socket) do
      {nil, _} ->
        state

      {ref, sockets} ->
        Process.demonitor(ref, [:flush])
        %{state | sockets: sockets} |> idle_when_empty()
    end
  end

  defp idle_when_empty(%{reservations: reservations, sockets: sockets} = state)
       when map_size(reservations) == 0 and map_size(sockets) == 0,
       do: %{state | idle: Process.send_after(self(), :idle, @idle_ms)}

  defp idle_when_empty(state), do: state
  defp cancel_idle(%{idle: nil} = state), do: state

  defp cancel_idle(%{idle: timer} = state) do
    Process.cancel_timer(timer)
    %{state | idle: nil}
  end
end
