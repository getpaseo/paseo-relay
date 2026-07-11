defmodule PaseoRelay.Registry do
  use GenServer

  alias PaseoRelay.Connection

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  def attach(pid, connection), do: GenServer.call(__MODULE__, {:attach, pid, connection})

  def forward(pid, payload, opcode),
    do: GenServer.cast(__MODULE__, {:forward, pid, payload, opcode})

  def detach(pid), do: GenServer.cast(__MODULE__, {:detach, pid})

  @impl true
  def init(_state) do
    Process.flag(:message_queue_data, :off_heap)
    {:ok, %{sessions: %{}, connections: %{}}}
  end

  @impl true
  def handle_call({:attach, pid, connection}, _from, state) do
    state = put_in(state.connections[pid], connection)
    {:reply, {:ok, self()}, attach(state, pid, connection)}
  end

  @impl true
  def handle_cast({:forward, pid, payload, opcode}, state) do
    {:noreply, forward(state, pid, payload, opcode)}
  end

  def handle_cast({:detach, pid}, state), do: {:noreply, detach(state, pid)}

  @impl true
  def handle_info({:nudge_control, server_id, connection_id}, state) do
    session = session(state, server_id)

    if waiting_for_data?(session, connection_id) do
      notify(session.control, %{type: "sync", connectionIds: Map.keys(session.clients)})
      Process.send_after(self(), {:reset_control, server_id, connection_id}, 5_000)
    end

    {:noreply, state}
  end

  def handle_info({:reset_control, server_id, connection_id}, state) do
    session = session(state, server_id)

    if waiting_for_data?(session, connection_id) do
      close(session.control, 1011, "Control unresponsive")
    end

    {:noreply, state}
  end

  defp attach(state, pid, %Connection{version: 1} = connection) do
    session = session(state, connection.server_id)
    old = session.v1[connection.role]
    close(old, 1008, "Replaced by new connection")

    put_session(state, connection.server_id, %{
      session
      | v1: Map.put(session.v1, connection.role, pid)
    })
  end

  defp attach(state, pid, %Connection{version: 2, role: :server, connection_id: ""} = connection) do
    session = session(state, connection.server_id)
    close(session.control, 1008, "Replaced by new connection")

    send(
      pid,
      {:relay_frame, :text,
       Jason.encode!(%{type: "sync", connectionIds: Map.keys(session.clients)})}
    )

    put_session(state, connection.server_id, %{session | control: pid})
  end

  defp attach(state, pid, %Connection{version: 2, role: :server} = connection) do
    session = session(state, connection.server_id)
    old = session.data[connection.connection_id]
    close(old, 1008, "Replaced by new connection")

    Enum.each(
      session.pending[connection.connection_id] || [],
      &send(pid, {:relay_frame, elem(&1, 0), elem(&1, 1)})
    )

    put_session(state, connection.server_id, %{
      session
      | data: Map.put(session.data, connection.connection_id, pid),
        pending: Map.delete(session.pending, connection.connection_id)
    })
  end

  defp attach(state, pid, %Connection{version: 2, role: :client} = connection) do
    session = session(state, connection.server_id)

    clients =
      Map.update(
        session.clients,
        connection.connection_id,
        MapSet.new([pid]),
        &MapSet.put(&1, pid)
      )

    notify(session.control, %{type: "connected", connectionId: connection.connection_id})

    Process.send_after(
      self(),
      {:nudge_control, connection.server_id, connection.connection_id},
      10_000
    )

    put_session(state, connection.server_id, %{session | clients: clients})
  end

  defp forward(state, pid, payload, opcode) do
    case state.connections[pid] do
      %Connection{version: 1} = connection ->
        target = session(state, connection.server_id).v1[opposite(connection.role)]
        deliver(target, opcode, payload)
        state

      %Connection{version: 2, role: :client} = connection ->
        session = session(state, connection.server_id)

        case session.data[connection.connection_id] do
          nil ->
            put_session(
              state,
              connection.server_id,
              buffer(session, connection.connection_id, {opcode, payload})
            )

          target ->
            deliver(target, opcode, payload)
            state
        end

      %Connection{version: 2, role: :server, connection_id: connection_id} = connection
      when connection_id != "" ->
        session = session(state, connection.server_id)
        Enum.each(session.clients[connection_id] || [], &deliver(&1, opcode, payload))
        state

      %Connection{version: 2, role: :server, connection_id: ""} ->
        answer_legacy_control_ping(pid, opcode, payload)
        state

      _ ->
        state
    end
  end

  defp detach(state, pid) do
    case Map.pop(state.connections, pid) do
      {nil, connections} ->
        %{state | connections: connections}

      {%Connection{} = connection, connections} ->
        %{state | connections: connections} |> detach_connection(pid, connection)
    end
  end

  defp detach_connection(state, pid, %Connection{version: 1} = connection) do
    session = session(state, connection.server_id)

    v1 =
      if session.v1[connection.role] == pid,
        do: Map.put(session.v1, connection.role, nil),
        else: session.v1

    put_session(state, connection.server_id, %{session | v1: v1})
  end

  defp detach_connection(state, pid, %Connection{version: 2, role: :client} = connection) do
    session = session(state, connection.server_id)
    remaining = MapSet.delete(session.clients[connection.connection_id] || MapSet.new(), pid)

    if MapSet.size(remaining) == 0 do
      close(session.data[connection.connection_id], 1001, "Client disconnected")
      notify(session.control, %{type: "disconnected", connectionId: connection.connection_id})

      put_session(state, connection.server_id, %{
        session
        | clients: Map.delete(session.clients, connection.connection_id),
          pending: Map.delete(session.pending, connection.connection_id)
      })
    else
      put_session(state, connection.server_id, %{
        session
        | clients: Map.put(session.clients, connection.connection_id, remaining)
      })
    end
  end

  defp detach_connection(
         state,
         pid,
         %Connection{version: 2, role: :server, connection_id: connection_id} = connection
       )
       when connection_id != "" do
    session = session(state, connection.server_id)

    if session.data[connection_id] == pid do
      Enum.each(session.clients[connection_id] || [], &close(&1, 1012, "Server disconnected"))

      put_session(state, connection.server_id, %{
        session
        | data: Map.delete(session.data, connection_id)
      })
    else
      state
    end
  end

  defp detach_connection(
         state,
         pid,
         %Connection{version: 2, role: :server, connection_id: ""} = connection
       ) do
    session = session(state, connection.server_id)

    if session.control == pid,
      do: put_session(state, connection.server_id, %{session | control: nil}),
      else: state
  end

  defp detach_connection(state, _pid, _connection), do: state

  defp session(state, server_id),
    do:
      Map.get(state.sessions, server_id, %{
        v1: %{server: nil, client: nil},
        control: nil,
        clients: %{},
        data: %{},
        pending: %{}
      })

  defp put_session(state, server_id, session) do
    if empty?(session) do
      %{state | sessions: Map.delete(state.sessions, server_id)}
    else
      put_in(state.sessions[server_id], session)
    end
  end

  defp empty?(session) do
    session.v1.server == nil and session.v1.client == nil and session.control == nil and
      map_size(session.clients) == 0 and map_size(session.data) == 0 and
      map_size(session.pending) == 0
  end

  defp waiting_for_data?(session, connection_id) do
    Map.has_key?(session.clients, connection_id) and not Map.has_key?(session.data, connection_id)
  end

  defp opposite(:server), do: :client
  defp opposite(:client), do: :server
  defp deliver(nil, _opcode, _payload), do: :ok

  defp deliver(pid, opcode, payload) do
    PaseoRelay.Metrics.inc(:frames_forwarded)
    PaseoRelay.Metrics.inc(:bytes_forwarded, byte_size(payload))
    send(pid, {:relay_frame, opcode, payload})
  end

  defp close(nil, _code, _reason), do: :ok
  defp close(pid, code, reason), do: send(pid, {:relay_close, code, reason})
  defp notify(nil, _message), do: :ok
  defp notify(pid, message), do: deliver(pid, :text, Jason.encode!(message))

  defp answer_legacy_control_ping(pid, :text, payload) do
    with {:ok, %{"type" => "ping"}} <- Jason.decode(payload) do
      send(
        pid,
        {:relay_frame, :text,
         Jason.encode!(%{type: "pong", ts: System.system_time(:millisecond)})}
      )
    end
  end

  defp answer_legacy_control_ping(_pid, _opcode, _payload), do: :ok

  defp buffer(session, connection_id, frame) do
    frames = (session.pending[connection_id] || []) ++ [frame]
    %{session | pending: Map.put(session.pending, connection_id, Enum.take(frames, -200))}
  end
end
