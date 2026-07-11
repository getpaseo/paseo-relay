defmodule PaseoRelay.PartitionClient do
  use WebSockex

  def start(url, observer) do
    {:ok, client} = WebSockex.start_link(url, __MODULE__, observer)
    Process.unlink(client)
    {:ok, client}
  end

  @impl true
  def handle_connect(_connection, observer) do
    send(observer, {:partition_open, self()})
    {:ok, observer}
  end

  @impl true
  def handle_frame(_frame, observer), do: {:ok, observer}

  @impl true
  def handle_disconnect(%{reason: reason}, observer) do
    send(observer, {:partition_closed, self(), reason})
    {:ok, observer}
  end
end
