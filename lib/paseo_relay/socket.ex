defmodule PaseoRelay.Socket do
  @moduledoc false

  @behaviour :cowboy_websocket

  alias PaseoRelay.Capacity
  alias PaseoRelay.Delivery
  alias PaseoRelay.Delivery.Deadline
  alias PaseoRelay.Delivery.Writer
  alias PaseoRelay.HandshakeValidation
  alias PaseoRelay.Ownership.Owner

  @impl true
  def init(request, options) do
    with true <- :cowboy_websocket.is_upgrade_request(request),
         {:ok, connection} <- connection(request),
         # Cowboy runs this callback in a request process; request.pid owns the
         # connection before and after the WebSocket upgrade.
         {:local, owner, reservation, admission} <-
           route(connection, options, Map.fetch!(request, :pid)) do
      state = %{
        admission: admission,
        config: options.config,
        connection: connection,
        owner: owner,
        reservation: reservation,
        pending: :queue.new()
      }

      {:cowboy_websocket, request, state, websocket_options(connection)}
    else
      {:reroute, _target} = decision ->
        PaseoRelay.Metrics.inc(:reroute_responses)
        reply(request, 409, reroute_headers(decision, options.reroute_header), "")

      {:unavailable, reason} ->
        reply(request, 503, %{}, Atom.to_string(reason))

      {:error, :capacity} ->
        PaseoRelay.Metrics.inc(:connection_rejections)
        reply(request, 503, %{}, "Relay connection capacity")

      {:error, :pressure} ->
        PaseoRelay.Metrics.inc(:connection_rejections)
        reply(request, 503, %{}, "Relay memory pressure")

      {:error, :configuration_mismatch} ->
        reply(request, 503, %{}, "Relay capacity configuration")

      {:error, :unavailable} ->
        reply(request, 503, %{}, "Relay capacity unavailable")

      false ->
        reply(request, 426, %{}, "Expected WebSocket upgrade")

      {:error, message} ->
        reply(request, 400, %{}, message)
    end
  end

  @impl true
  def websocket_init(state) do
    Process.flag(:message_queue_data, :off_heap)

    Process.flag(:max_heap_size, %{
      size: state.config.websocket_max_heap_words,
      include_shared_binaries: true,
      kill: true
    })

    with {:ok, capacity} <-
           Capacity.attach_connection(
             state.admission,
             state.config.capacity_mutation_timeout_ms
           ),
         {:ok, writer} <-
           Writer.start(
             self(),
             state.config.delivery_timeout_ms,
             state.config.control_queue_bytes
           ) do
      state =
        Map.put(state, :admission, %{
          token: state.admission,
          monitor: Process.monitor(capacity)
        })

      attach_writer(writer, state)
    else
      {:error, _reason} -> {[{:close, 1012, "Session expired"}], state}
    end
  end

  @impl true
  def websocket_handle(
        frame,
        %{connection: %{version: 2, role: :server, connection_id: ""}} = state
      ) do
    handle_control_input(frame, state)
  end

  def websocket_handle({opcode, payload}, state) when opcode in [:text, :binary] do
    PaseoRelay.Metrics.observe_frame(byte_size(payload))

    case PaseoRelay.Capacity.admit_message(
           byte_size(payload),
           state.config.capacity_mutation_timeout_ms
         ) do
      {:ok, token} -> admit_input(opcode, payload, token, state)
      {:error, _reason} -> {[{:close, 1013, "Relay ingress capacity"}], state}
    end
  end

  def websocket_handle(_control, state), do: {[], state}

  @impl true
  def websocket_info({:relay_frame, _writer, _reference, opcode, payload}, state) do
    {[{opcode, payload}], state}
  end

  def websocket_info({:relay_write_barrier, writer, reference}, state) do
    Writer.acknowledge(writer, reference)
    {[], state}
  end

  def websocket_info({reference, result}, %{delivery: %{ref: reference}} = state) do
    Process.demonitor(reference, [:flush])
    state = finish_delivery(state)

    case result do
      :ok -> continue_or_resume(state)
      {:error, :attach_timeout} -> {[{:close, 1013, "Data route unavailable"}], state}
      {:error, _reason} -> {[{:close, 1013, "Delivery unavailable"}], state}
    end
  end

  def websocket_info(
        {:DOWN, reference, :process, _pid, _reason},
        %{delivery: %{ref: reference}} = state
      ) do
    {[{:close, 1013, "Delivery unavailable"}], finish_delivery(state)}
  end

  def websocket_info(:relay_memory_pressure, state),
    do: {[{:close, 1013, "Relay memory pressure"}], cancel_delivery(state)}

  def websocket_info({:DOWN, ref, :process, _owner, _reason}, %{owner_ref: ref} = state) do
    {[{:close, 1012, "Session owner moved"}], cancel_delivery(state)}
  end

  def websocket_info({:DOWN, ref, :process, _writer, _reason}, %{writer_ref: ref} = state) do
    {[{:close, 1013, "Delivery unavailable"}], cancel_delivery(state)}
  end

  def websocket_info(
        {:DOWN, ref, :process, _budget, _reason},
        %{admission: %{monitor: ref}} = state
      ) do
    {[{:close, 1013, "Relay capacity unavailable"}], cancel_delivery(state)}
  end

  def websocket_info({:relay_close, code, reason}, state) do
    {[{:close, code, reason}], cancel_delivery(state)}
  end

  def websocket_info(_message, state), do: {[], state}

  @impl true
  def terminate(reason, _request, %{owner: owner} = state) do
    state = drain_before_detach(reason, state)

    if delivery = state[:delivery] do
      stop_delivery_task(delivery)
      PaseoRelay.Capacity.cancel_message(delivery.token)
    end

    Capacity.release_connection(admission_token(state.admission))
    Owner.detach(owner, self())
  end

  def terminate(_reason, _request, _state), do: :ok

  # A peer may send its last messages and its Close in one write. Cowboy parses that
  # Close and terminates without returning to this process, so the messages are
  # still in flight or pending. On a normal remote Close, deliver them in order
  # before detaching. Queued messages start only before one shared deadline; like an
  # open socket, the drain then waits for each delivery to end at its own deadline
  # instead of cancelling it, so Owner retirement and Writer shedding still happen.
  # Stop at the first failed delivery and on any event that would already close this
  # socket abnormally. Cowboy has answered the Close, so this socket's own outbound
  # traffic is not serviced.
  defp drain_before_detach(:remote, state), do: drain_on_close(state)
  defp drain_before_detach({:remote, _code, _payload}, state), do: drain_on_close(state)
  defp drain_before_detach(_reason, state), do: state

  defp drain_on_close(state) do
    drain(state, Deadline.after_ms(state.config.delivery_timeout_ms))
  end

  defp drain(%{delivery: %{ref: reference}} = state, deadline) do
    owner_ref = state[:owner_ref]
    writer_ref = state[:writer_ref]
    budget_ref = admission_monitor(state.admission)

    receive do
      {^reference, :ok} ->
        Process.demonitor(reference, [:flush])
        state |> finish_delivery() |> drain_next(deadline)

      {^reference, _error} ->
        Process.demonitor(reference, [:flush])
        finish_delivery(state)

      {:DOWN, ^reference, :process, _pid, _reason} ->
        finish_delivery(state)

      :relay_memory_pressure ->
        state

      {:relay_close, _code, _reason} ->
        state

      {:DOWN, ^owner_ref, :process, _owner, _reason} ->
        state

      {:DOWN, ^writer_ref, :process, _writer, _reason} ->
        state

      {:DOWN, ^budget_ref, :process, _budget, _reason} ->
        state
    end
  end

  defp drain(state, deadline), do: drain_next(state, deadline)

  defp drain_next(state, deadline) do
    with {{:value, {opcode, payload, token}}, pending} <- :queue.out(state.pending),
         true <- Deadline.remaining(deadline) > 0,
         {:ok, state} <-
           start_delivery(opcode, payload, token, %{state | pending: pending}, deadline) do
      drain(state, deadline)
    else
      _stop -> state
    end
  end

  defp admit_input(opcode, payload, token, %{connection: %{role: :client}} = state) do
    case HandshakeValidation.check(opcode, payload) do
      :not_handshake ->
        forward_admitted_input(opcode, payload, token, state)

      {:accept, type} ->
        PaseoRelay.Metrics.observe_handshake(:accepted, state.connection.version, type)
        forward_admitted_input(opcode, payload, token, state)

      {:reject, type} ->
        PaseoRelay.Metrics.observe_handshake(:rejected, state.connection.version, type)
        :ok = PaseoRelay.Capacity.finish_message(token)
        {[{:close, 1008, "Invalid handshake key"}], state}
    end
  end

  defp admit_input(opcode, payload, token, state),
    do: forward_admitted_input(opcode, payload, token, state)

  defp forward_admitted_input(opcode, payload, token, %{delivery: _delivery} = state) do
    pending = :queue.in({opcode, payload, token}, state.pending)
    {[{:active, false}], %{state | pending: pending}}
  end

  defp forward_admitted_input(opcode, payload, token, state) do
    case start_delivery(opcode, payload, token, state) do
      {:ok, state} -> {[{:active, false}], state}
      {:error, state} -> {[{:close, 1013, "Relay memory pressure"}], state}
    end
  end

  # The Capacity call always keeps its configured timeout: an expiry kills the ledger.
  # A close drain passes its own deadline so every delivery shares one bound.
  defp start_delivery(opcode, payload, token, state, drain_deadline \\ nil) do
    case PaseoRelay.Capacity.start_delivery(token, state.config.capacity_mutation_timeout_ms) do
      :ok ->
        source = self()
        deadline = drain_deadline || Deadline.after_ms(state.config.delivery_timeout_ms)
        owner = state.owner
        attach_timeout = state.config.data_attach_timeout_ms

        task =
          Task.async(fn ->
            deliver_input(payload, opcode, owner, attach_timeout, source, deadline)
          end)

        delivery = %{
          ref: task.ref,
          pid: task.pid,
          token: token
        }

        {:ok, Map.put(state, :delivery, delivery)}

      {:error, _reason} ->
        PaseoRelay.Capacity.cancel_message(token)
        {:error, state}
    end
  end

  defp continue_or_resume(state) do
    case :queue.out(state.pending) do
      {{:value, {opcode, payload, token}}, pending} ->
        state = %{state | pending: pending}

        case start_delivery(opcode, payload, token, state) do
          {:ok, state} -> {[], state}
          {:error, state} -> {[{:close, 1013, "Relay memory pressure"}], state}
        end

      {:empty, _pending} ->
        {[{:active, true}], state}
    end
  end

  # Takes only what it needs so a task does not copy this socket's pending queue.
  defp deliver_input(payload, opcode, owner, attach_timeout, source, deadline) do
    case Owner.destinations(owner, source, deadline, attach_timeout) do
      {:ok, destinations} -> Delivery.deliver(destinations, opcode, payload, deadline)
      {:error, :attach_timeout} -> {:error, :attach_timeout}
      {:error, _reason} -> {:error, :delivery_unavailable}
    end
  end

  defp handle_control_input({:text, payload}, state) do
    PaseoRelay.Metrics.observe_frame(byte_size(payload))

    case PaseoRelay.Capacity.admit_message(
           byte_size(payload),
           state.config.capacity_mutation_timeout_ms
         ) do
      {:ok, token} ->
        result = handle_admitted_control(payload, state)
        :ok = PaseoRelay.Capacity.finish_message(token)
        result

      {:error, _reason} ->
        {[{:close, 1013, "Relay ingress capacity"}], state}
    end
  end

  defp handle_control_input(_frame, state), do: {[], state}

  defp finish_delivery(state) do
    delivery = state.delivery
    PaseoRelay.Capacity.finish_message(delivery.token)
    Map.delete(state, :delivery)
  end

  defp cancel_delivery(%{delivery: delivery} = state) do
    stop_delivery_task(delivery)
    finish_delivery(state)
  end

  defp cancel_delivery(state), do: state

  defp stop_delivery_task(delivery) do
    Process.unlink(delivery.pid)
    Process.exit(delivery.pid, :kill)
    Process.demonitor(delivery.ref, [:flush])
  end

  defp connection(request) do
    query =
      request
      |> :cowboy_req.parse_qs()
      |> Map.new(fn
        {key, true} -> {key, ""}
        {key, value} -> {key, value}
      end)

    PaseoRelay.Connection.from_query(query)
  end

  defp admit({namespace, limit}, holder, timeout),
    do: Capacity.admit_connection(namespace, limit, holder, timeout)

  defp route(connection, options, holder) do
    case PaseoRelay.Ownership.resolve(connection.server_id) do
      {:reroute, _target} = decision ->
        decision

      decision when decision in [:local, :unowned] ->
        admit_and_route(connection, options, holder)
    end
  end

  defp admit_and_route(connection, options, holder) do
    with {:ok, admission} <-
           admit(options.connection_budget, holder, options.config.capacity_mutation_timeout_ms) do
      decision =
        PaseoRelay.Ownership.route(
          connection.server_id,
          options.ownership_target,
          options.config.minimum_cluster_size
        )

      case decision do
        {:local, owner, reservation} ->
          {:local, owner, reservation, admission}

        _not_local ->
          Capacity.release_connection(admission)
          decision
      end
    end
  end

  defp admission_token(%{token: token}), do: token
  defp admission_token(token), do: token

  defp admission_monitor(%{monitor: monitor}), do: monitor
  defp admission_monitor(_token), do: nil

  defp websocket_options(%{version: 2, role: :server, connection_id: ""}) do
    websocket_options_with_limit(PaseoRelay.Protocol.maximum_control_payload_bytes())
  end

  defp websocket_options(_connection) do
    websocket_options_with_limit(PaseoRelay.Protocol.maximum_message_payload_bytes())
  end

  defp websocket_options_with_limit(max_frame_size) do
    %{
      active_n: 1,
      compress: false,
      idle_timeout: :infinity,
      max_frame_size: max_frame_size
    }
  end

  defp handle_admitted_control(payload, state) do
    with {:ok, %{"type" => "ping"}} <- Jason.decode(payload) do
      pong = Jason.encode!(%{type: "pong", ts: System.system_time(:millisecond)})

      case Owner.control(state.owner, self(), pong) do
        :ok -> {[], state}
        :closed -> {[{:close, 1013, "Delivery unavailable"}], state}
        {:error, _reason} -> {[{:close, 1013, "Delivery unavailable"}], state}
      end
    else
      _ -> {[], state}
    end
  end

  defp reply(request, status, headers, body) do
    request = :cowboy_req.reply(status, headers, body, request)
    {:ok, request, nil}
  end

  defp reroute_headers(decision, header) do
    decision
    |> PaseoRelay.Reroute.headers(header)
    |> Map.new(fn {name, value} -> {to_string(name), value} end)
  end

  defp monitor(process) do
    if Process.alive?(process), do: {:ok, Process.monitor(process)}, else: {:error, :closed}
  end

  defp attach_writer(writer, state) do
    case Owner.attach(state.owner, state.reservation, self(), state.connection, writer) do
      :ok ->
        with {:ok, owner_ref} <- monitor(state.owner),
             {:ok, writer_ref} <- monitor(writer) do
          {[],
           state
           |> Map.put(:writer, writer)
           |> Map.put(:writer_ref, writer_ref)
           |> Map.put(:owner_ref, owner_ref)}
        else
          _reason ->
            Process.exit(writer, :normal)
            {[{:close, 1012, "Session expired"}], state}
        end

      :closed ->
        Process.exit(writer, :normal)
        {[{:close, 1012, "Session expired"}], state}
    end
  end
end
