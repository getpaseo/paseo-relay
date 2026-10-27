defmodule PaseoRelay.StaticCluster do
  @moduledoc "Connects configured BEAM peers independently of DNS discovery."
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @impl true
  def init(options) do
    state = %{
      nodes: Keyword.fetch!(options, :nodes),
      interval: Keyword.get(options, :interval, 5_000)
    }

    send(self(), :connect)
    {:ok, state}
  end

  @impl true
  def handle_info(:connect, state) do
    for peer <- state.nodes, peer != node(), peer not in Node.list() do
      Node.connect(peer)
    end

    Process.send_after(self(), :connect, state.interval)
    {:noreply, state}
  end
end
