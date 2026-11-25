defmodule PaseoRelay.Ownership do
  @moduledoc false

  alias PaseoRelay.Ownership.Owner

  @scope :paseo_relay_owners

  def route(server_id, target),
    do: route(server_id, target, configured_minimum_cluster_size())

  def route(server_id, target, minimum_cluster_size) do
    case lookup(server_id) do
      {owner, owner_target} -> route_owner(owner, owner_target)
      :undefined -> claim_route(server_id, target, minimum_cluster_size)
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

  def ready?, do: ready?(configured_minimum_cluster_size())

  def ready?(minimum_cluster_size),
    do:
      length(:syn.subcluster_nodes(:registry, @scope)) + 1 >=
        minimum_cluster_size

  defp claim_route(server_id, target, minimum_cluster_size) do
    cond do
      draining?() -> {:unavailable, :draining}
      not ready?(minimum_cluster_size) -> {:unavailable, :cluster}
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

  defp configured_minimum_cluster_size do
    :paseo_relay
    |> Application.get_env(:runtime, PaseoRelay.Config.defaults())
    |> PaseoRelay.Config.normalize()
    |> Map.fetch!(:minimum_cluster_size)
  end
end

defmodule PaseoRelay.Ownership.Owner do
  use GenServer

  alias PaseoRelay.Connection
  alias PaseoRelay.Delivery.Deadline
  alias PaseoRelay.Delivery.Writer

  @reservation_ms 5_000
  @idle_ms 30_000
  @call_timeout_ms 5_000

  def start(server_id, target), do: GenServer.start(__MODULE__, {server_id, target})
  def reserve(owner), do: call(owner, :reserve)

  def attach(owner, reservation, socket),
    do: call(owner, {:attach, reservation, socket})

  def attach(owner, reservation, socket, connection, writer),
    do: call(owner, {:attach, reservation, socket, connection, writer})

  def cancel(owner, reservation), do: GenServer.cast(owner, {:cancel, reservation})

  @doc """
  Stops the Owner now, without the idle grace. Syn unregisters the session and
  attached sockets close with `1012 Session owner moved`, so the daemon
  reconnects and can claim the session on its home node.
  """
  def relocate(owner), do: GenServer.cast(owner, :relocate)
  def detach(owner, socket), do: GenServer.cast(owner, {:detach, socket})
  def legacy(owner, socket), do: call(owner, {:legacy, socket})

  def destinations(owner, socket, deadline, attach_timeout) do
    case Deadline.remaining(deadline) do
      0 -> {:error, :owner_timeout}
      timeout -> GenServer.call(owner, {:destinations, socket, deadline, attach_timeout}, timeout)
    end
  catch
    :exit, {:timeout, _call} ->
      Process.exit(owner, :kill)
      {:error, :owner_timeout}

    :exit, _reason ->
      {:error, :owner_closed}
  end

  def control(owner, socket, payload), do: call(owner, {:control, socket, payload})

  @impl true
  def init({server_id, target}) do
    case :syn.register(:paseo_relay_owners, server_id, self(), target) do
      :ok ->
        PaseoRelay.Placement.session_started(server_id)

        {:ok,
         %{
           server_id: server_id,
           client_count: 0,
           reservations: %{},
           sockets: %{},
           v1: %{server: nil, client: nil},
           control: nil,
           clients: %{},
           data: %{},
           waiting: %{},
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
    attach_reservation(state, token, socket, nil)
  end

  def handle_call({:attach, token, socket, connection, writer}, _from, state) do
    attach_reservation(state, token, socket, {connection, writer})
  end

  def handle_call({:destinations, socket, deadline, attach_timeout}, from, state) do
    timeout = min(Deadline.remaining(deadline), attach_timeout)

    case {timeout, state.sockets[socket]} do
      {0, _socket_state} ->
        {:reply, {:error, :owner_timeout}, state}

      {_timeout, %{connection: %Connection{version: 1} = connection}} ->
        target = state.v1[opposite(connection.role)]
        {:reply, {:ok, writers(state, [target])}, state}

      {timeout, %{connection: %Connection{version: 2, role: :client} = connection}} ->
        case state.data[connection.connection_id] do
          nil -> wait_for_data(state, from, socket, connection.connection_id, timeout)
          target -> {:reply, {:ok, writers(state, [target])}, state}
        end

      {_timeout, %{connection: %Connection{version: 2, role: :server, connection_id: id}}}
      when id != "" ->
        targets = state.clients[id] || MapSet.new()
        {:reply, {:ok, writers(state, targets)}, state}

      {_timeout, %{connection: %Connection{version: 2, role: :server, connection_id: ""}}} ->
        {:reply, {:ok, :control}, state}

      {_timeout, _missing} ->
        {:reply, {:error, :detached}, state}
    end
  end

  def handle_call({:legacy, socket}, _from, state) do
    {:reply, :ok, %{state | legacy: Process.monitor(socket)}}
  end

  def handle_call({:control, socket, payload}, _from, state) do
    case get_in(state.sockets, [socket, :writer]) do
      nil -> {:reply, {:error, :detached}, state}
      writer -> {:reply, Writer.control(writer, payload), state}
    end
  end

  defp attach_reservation(state, token, socket, attachment) do
    case Map.pop(state.reservations, token) do
      {nil, _} ->
        {:reply, :closed, state}

      {timer, reservations} ->
        Process.cancel_timer(timer)

        socket_state = %{
          monitor: Process.monitor(socket),
          connection: attachment && elem(attachment, 0),
          writer: attachment && elem(attachment, 1)
        }

        state = %{
          cancel_idle(state)
          | reservations: reservations,
            sockets: Map.put(state.sockets, socket, socket_state)
        }

        state =
          if attachment, do: attach_connection(state, socket, elem(attachment, 0)), else: state

        {:reply, :ok, track_clients(state)}
    end
  end

  @impl true
  def handle_cast({:cancel, token}, state) do
    case Map.pop(state.reservations, token) do
      {nil, _reservations} ->
        {:noreply, state}

      {timer, reservations} ->
        Process.cancel_timer(timer)
        {:noreply, %{state | reservations: reservations} |> idle_when_empty()}
    end
  end

  def handle_cast({:detach, socket}, state), do: {:noreply, remove_socket(state, socket)}
  def handle_cast(:relocate, state), do: {:stop, :normal, state}

  @impl true
  def handle_info({:expired, token}, state),
    do:
      {:noreply, state |> Map.update!(:reservations, &Map.delete(&1, token)) |> idle_when_empty()}

  def handle_info({:data_timeout, reference}, state) do
    case Map.pop(state.waiting, reference) do
      {nil, _waiting} ->
        {:noreply, state}

      {%{from: from, caller_ref: caller_ref}, waiting} ->
        Process.demonitor(caller_ref, [:flush])
        GenServer.reply(from, {:error, :attach_timeout})
        {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_info({:nudge_control, connection_id}, state) do
    if waiting_for_data?(state, connection_id) do
      notify(state, state.control, %{type: "sync", connectionIds: Map.keys(state.clients)})
      Process.send_after(self(), {:reset_control, connection_id}, 5_000)
    end

    {:noreply, state}
  end

  def handle_info({:reset_control, connection_id}, state) do
    if waiting_for_data?(state, connection_id) do
      close(state, state.control, 1011, "Control unresponsive")
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, socket, _}, state) do
    cond do
      state[:legacy] == ref -> {:stop, :normal, state}
      get_in(state.sockets, [socket, :monitor]) == ref -> {:noreply, remove_socket(state, socket)}
      waiting_caller?(state, ref) -> {:noreply, remove_waiting_caller(state, ref)}
      true -> {:noreply, state}
    end
  end

  def handle_info(:idle, %{reservations: reservations, sockets: sockets} = state)
      when map_size(reservations) == 0 and map_size(sockets) == 0, do: {:stop, :normal, state}

  def handle_info(:idle, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: PaseoRelay.Placement.session_stopped(state.server_id)

  defp remove_socket(state, socket) do
    case Map.pop(state.sockets, socket) do
      {nil, _} ->
        state

      {%{monitor: reference, connection: connection}, sockets} ->
        Process.demonitor(reference, [:flush])

        %{state | sockets: sockets}
        |> detach_connection(socket, connection)
        |> reject_source_waiters(socket)
        |> track_clients()
        |> idle_when_empty()
    end
  end

  defp attach_connection(state, socket, %Connection{version: 1} = connection) do
    old = state.v1[connection.role]
    close(state, old, 1008, "Replaced by new connection")
    %{state | v1: Map.put(state.v1, connection.role, socket)}
  end

  defp attach_connection(
         state,
         socket,
         %Connection{version: 2, role: :server, connection_id: ""}
       ) do
    close(state, state.control, 1008, "Replaced by new connection")
    notify(state, socket, %{type: "sync", connectionIds: Map.keys(state.clients)})
    %{state | control: socket}
  end

  defp attach_connection(state, socket, %Connection{version: 2, role: :server} = connection) do
    old = state.data[connection.connection_id]
    close(state, old, 1008, "Replaced by new connection")

    state
    |> Map.put(:data, Map.put(state.data, connection.connection_id, socket))
    |> release_waiters(connection.connection_id, socket)
  end

  defp attach_connection(state, socket, %Connection{version: 2, role: :client} = connection) do
    clients =
      Map.update(
        state.clients,
        connection.connection_id,
        MapSet.new([socket]),
        &MapSet.put(&1, socket)
      )

    notify(state, state.control, %{type: "connected", connectionId: connection.connection_id})
    Process.send_after(self(), {:nudge_control, connection.connection_id}, 10_000)
    %{state | clients: clients}
  end

  defp detach_connection(state, _socket, nil), do: state

  defp detach_connection(state, socket, %Connection{version: 1} = connection) do
    if state.v1[connection.role] == socket do
      %{state | v1: Map.put(state.v1, connection.role, nil)}
    else
      state
    end
  end

  defp detach_connection(state, socket, %Connection{version: 2, role: :client} = connection) do
    remaining = MapSet.delete(state.clients[connection.connection_id] || MapSet.new(), socket)

    if MapSet.size(remaining) == 0 do
      close(state, state.data[connection.connection_id], 1001, "Client disconnected")

      notify(state, state.control, %{
        type: "disconnected",
        connectionId: connection.connection_id
      })

      %{state | clients: Map.delete(state.clients, connection.connection_id)}
    else
      %{state | clients: Map.put(state.clients, connection.connection_id, remaining)}
    end
  end

  defp detach_connection(
         state,
         socket,
         %Connection{version: 2, role: :server, connection_id: connection_id}
       )
       when connection_id != "" do
    if state.data[connection_id] == socket do
      Enum.each(
        state.clients[connection_id] || [],
        &close(state, &1, 1012, "Server disconnected")
      )

      %{state | data: Map.delete(state.data, connection_id)}
    else
      state
    end
  end

  defp detach_connection(
         state,
         socket,
         %Connection{version: 2, role: :server, connection_id: ""}
       ) do
    if state.control == socket, do: %{state | control: nil}, else: state
  end

  defp wait_for_data(state, from, source, connection_id, timeout) do
    reference = make_ref()
    timer = Process.send_after(self(), {:data_timeout, reference}, timeout)
    caller_ref = Process.monitor(elem(from, 0))

    waiter = %{
      from: from,
      source: source,
      connection_id: connection_id,
      timer: timer,
      caller_ref: caller_ref
    }

    {:noreply, %{state | waiting: Map.put(state.waiting, reference, waiter)}}
  end

  defp release_waiters(state, connection_id, destination) do
    {matching, waiting} =
      Enum.split_with(state.waiting, fn {_reference, waiter} ->
        waiter.connection_id == connection_id
      end)

    target_writers = writers(state, [destination])

    Enum.each(matching, fn {_reference, waiter} ->
      Process.cancel_timer(waiter.timer)
      Process.demonitor(waiter.caller_ref, [:flush])
      GenServer.reply(waiter.from, {:ok, target_writers})
    end)

    %{state | waiting: Map.new(waiting)}
  end

  defp reject_source_waiters(state, source) do
    {matching, waiting} =
      Enum.split_with(state.waiting, fn {_reference, waiter} -> waiter.source == source end)

    Enum.each(matching, fn {_reference, waiter} ->
      Process.cancel_timer(waiter.timer)
      Process.demonitor(waiter.caller_ref, [:flush])
      GenServer.reply(waiter.from, {:error, :detached})
    end)

    %{state | waiting: Map.new(waiting)}
  end

  defp writers(state, sockets) do
    sockets
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&get_in(state.sockets, [&1, :writer]))
    |> Enum.reject(&is_nil/1)
  end

  defp waiting_for_data?(state, connection_id) do
    Map.has_key?(state.clients, connection_id) and not Map.has_key?(state.data, connection_id)
  end

  defp waiting_caller?(state, reference) do
    Enum.any?(state.waiting, fn {_key, waiter} -> waiter.caller_ref == reference end)
  end

  defp remove_waiting_caller(state, reference) do
    {matching, waiting} =
      Enum.split_with(state.waiting, fn {_key, waiter} -> waiter.caller_ref == reference end)

    Enum.each(matching, fn {_key, waiter} -> Process.cancel_timer(waiter.timer) end)
    %{state | waiting: Map.new(waiting)}
  end

  # The heal scan reads this count from the Placement index; idle sessions move first.
  defp track_clients(state) do
    count = client_count(state)

    if count != state.client_count,
      do: PaseoRelay.Placement.session_clients(state.server_id, count)

    %{state | client_count: count}
  end

  defp client_count(state) do
    v1_clients = if state.v1.client, do: 1, else: 0

    Enum.reduce(state.clients, v1_clients, fn {_connection_id, sockets}, count ->
      count + MapSet.size(sockets)
    end)
  end

  defp opposite(:server), do: :client
  defp opposite(:client), do: :server
  defp close(_state, nil, _code, _reason), do: :ok

  defp close(state, socket, code, reason) do
    if writer = get_in(state.sockets, [socket, :writer]), do: Writer.close(writer, code, reason)
  end

  defp notify(_state, nil, _message), do: :ok

  defp notify(state, socket, message) do
    payload = Jason.encode!(message)

    if writer = get_in(state.sockets, [socket, :writer]), do: Writer.control(writer, payload)
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
    :exit, {:timeout, _call} ->
      Process.exit(owner, :kill)
      :closed

    :exit, _reason ->
      :closed
  end
end
