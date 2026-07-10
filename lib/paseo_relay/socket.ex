defmodule PaseoRelay.Socket do
  @behaviour WebSock

  @impl true
  def init(connection) do
    :ok = PaseoRelay.Registry.attach(self(), connection)
    {:ok, connection}
  end

  @impl true
  def handle_in({payload, [opcode: opcode]}, connection) do
    :ok = PaseoRelay.Registry.forward(self(), payload, opcode)
    {:ok, connection}
  end

  @impl true
  def handle_info({:relay_frame, opcode, payload}, connection),
    do: {:push, {opcode, payload}, connection}

  def handle_info({:relay_close, code, reason}, connection),
    do: {:stop, :normal, {code, reason}, connection}

  @impl true
  def terminate(_reason, _connection), do: PaseoRelay.Registry.detach(self())
end
