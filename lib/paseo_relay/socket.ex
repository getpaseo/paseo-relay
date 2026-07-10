defmodule PaseoRelay.Socket do
  @behaviour WebSock

  @impl true
  def init(%{connection: connection, owner: owner, reservation: reservation} = state) do
    with :ok <- PaseoRelay.Ownership.Owner.attach(owner, reservation, self()),
         :ok <- PaseoRelay.Registry.attach(self(), connection) do
      PaseoRelay.Metrics.inc(:active_websockets)
      {:ok, state}
    else
      _ -> {:stop, :normal, {1012, "Session expired"}, state}
    end
  end

  @impl true
  def handle_in({payload, [opcode: opcode]}, state) do
    :ok = PaseoRelay.Registry.forward(self(), payload, opcode)
    {:ok, state}
  end

  @impl true
  def handle_info({:relay_frame, opcode, payload}, state), do: {:push, {opcode, payload}, state}

  def handle_info({:relay_close, code, reason}, state),
    do: {:stop, :normal, {code, reason}, state}

  @impl true
  def terminate(_reason, %{owner: owner}) do
    PaseoRelay.Registry.detach(self())
    PaseoRelay.Ownership.Owner.detach(owner, self())
    PaseoRelay.Metrics.dec(:active_websockets)
  end
end
