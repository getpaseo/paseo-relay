defmodule PaseoRelay.Registry do
  use GenServer

  alias PaseoRelay.Connection

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  def attach(pid, connection), do: GenServer.call(__MODULE__, {:attach, pid, connection})

  def forward(pid, payload, opcode),
    do: GenServer.cast(__MODULE__, {:forward, pid, payload, opcode})

  def detach(pid), do: GenServer.cast(__MODULE__, {:detach, pid})

  @impl true
  def init(_state), do: {:ok, %{sessions: %{}, connections: %{}}}

  @impl true
  def handle_call({:attach, pid, connection}, _from, state) do
    state = put_in(state.connections[pid], connection)
    {:reply, :ok, attach(state, pid, connection)}
  end

  @impl true
  def handle_cast({:forward, pid, payload, opcode}, state) do
    {:noreply, forward(state, pid, payload, opcode)}
  end

  def handle_cast({:detach, pid}, state), do: {:noreply, detach(state, pid)}

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

  defp put_session(state, server_id, session), do: put_in(state.sessions[server_id], session)
  defp opposite(:server), do: :client
  defp opposite(:client), do: :server
  defp deliver(nil, _opcode, _payload), do: :ok
  defp deliver(pid, opcode, payload), do: send(pid, {:relay_frame, opcode, payload})
  defp close(nil, _code, _reason), do: :ok
  defp close(pid, code, reason), do: send(pid, {:relay_close, code, reason})
  defp notify(nil, _message), do: :ok
  defp notify(pid, message), do: deliver(pid, :text, Jason.encode!(message))

  defp buffer(session, connection_id, frame) do
    frames = (session.pending[connection_id] || []) ++ [frame]
    %{session | pending: Map.put(session.pending, connection_id, Enum.take(frames, -200))}
  end
end
