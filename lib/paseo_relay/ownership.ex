defmodule PaseoRelay.Ownership do
  @moduledoc false

  alias PaseoRelay.Ownership.Owner

  @scope :paseo_relay_owners

  def route(server_id, target) do
    case lookup(server_id) do
      {owner, owner_target} -> route_owner(owner, owner_target)
      :undefined -> claim_route(server_id, target)
    end
  end

  # Kept for the existing ownership contract.
  def claim(server_id, target), do: claim(server_id, target, self())

  def claim(server_id, target, session_owner) do
    case route(server_id, target) do
      {:local, owner, reservation} ->
        with :ok <- Owner.attach(owner, reservation, session_owner),
             :ok <- Owner.legacy(owner, session_owner) do
          :local
        else
          :closed -> {:unavailable, :owner}
        end

      other ->
        other
    end
  end

  def resolve(server_id) do
    case lookup(server_id) do
      :undefined -> :unowned
      {owner, _target} when node(owner) == node() -> :local
      {_owner, target} -> {:reroute, target}
    end
  end

  def owner_pid(server_id) do
    case lookup(server_id) do
      {owner, _target} -> owner
      :undefined -> :undefined
    end
  end

  def ready?,
    do:
      length(:syn.subcluster_nodes(:registry, @scope)) + 1 >=
        Application.get_env(:paseo_relay, :minimum_cluster_size, 1)

  defp claim_route(server_id, target) do
    cond do
      draining?() -> {:unavailable, :draining}
      not ready?() -> {:unavailable, :cluster}
      true -> start_or_route(server_id, target)
    end
  end

  defp start_or_route(server_id, target) do
    case Owner.start(server_id, target) do
      {:ok, owner} -> route_owner(owner, target)
      {:error, _reason} -> route_lookup(server_id)
    end
  end

  defp route_lookup(server_id) do
    case lookup(server_id) do
      {owner, target} -> route_owner(owner, target)
      :undefined -> {:unavailable, :owner}
    end
  end

  defp route_owner(owner, _target) when is_pid(owner) and node(owner) == node() do
    case Owner.reserve(owner) do
      {:ok, reservation} -> {:local, owner, reservation}
      :closed -> {:unavailable, :owner}
    end
  end

  defp route_owner(_owner, target), do: {:reroute, target}

  defp lookup(server_id), do: :syn.lookup(@scope, server_id)

  defp draining? do
    if Process.whereis(PaseoRelay.Drain), do: PaseoRelay.Drain.draining?(), else: false
  end
end

defmodule PaseoRelay.Ownership.Owner do
  use GenServer

  @reservation_ms 5_000
  @idle_ms 30_000
  @call_timeout_ms 5_000

  def start(server_id, target), do: GenServer.start(__MODULE__, {server_id, target})
  def reserve(owner), do: call(owner, :reserve)

  def attach(owner, reservation, socket),
    do: call(owner, {:attach, reservation, socket})

  def detach(owner, socket), do: GenServer.cast(owner, {:detach, socket})
  def legacy(owner, socket), do: call(owner, {:legacy, socket})

  @impl true
  def init({server_id, target}) do
    case :syn.register(:paseo_relay_owners, server_id, self(), target) do
      :ok ->
        {:ok,
         %{
           reservations: %{},
           sockets: %{},
           idle: nil,
           legacy: nil
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
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

  def handle_call({:legacy, socket}, _from, state) do
    {:reply, :ok, %{state | legacy: Process.monitor(socket)}}
  end

  @impl true
  def handle_cast({:detach, socket}, state), do: {:noreply, remove_socket(state, socket)}

  @impl true
  def handle_info({:expired, token}, state),
    do:
      {:noreply, state |> Map.update!(:reservations, &Map.delete(&1, token)) |> idle_when_empty()}

  def handle_info({:DOWN, ref, :process, socket, _}, state) do
    cond do
      state[:legacy] == ref -> {:stop, :normal, state}
      Map.get(state.sockets, socket) == ref -> {:noreply, remove_socket(state, socket)}
      true -> {:noreply, state}
    end
  end

  def handle_info(:idle, %{reservations: reservations, sockets: sockets} = state)
      when map_size(reservations) == 0 and map_size(sockets) == 0, do: {:stop, :normal, state}

  def handle_info(:idle, state), do: {:noreply, state}

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

  defp call(owner, message) do
    GenServer.call(owner, message, @call_timeout_ms)
  catch
    :exit, _reason -> :closed
  end
end
