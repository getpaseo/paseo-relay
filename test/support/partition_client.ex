defmodule PaseoRelay.PartitionClient do
  use WebSockex

  def start(url, observer) do
    {:ok, client} = WebSockex.start_link(url, __MODULE__, observer)
    Process.unlink(client)
    {:ok, client}
  end

  # Returns `{:error, %WebSockex.RequestError{code: status}}` for a refused upgrade.
  def connect(url, observer, headers \\ []),
    do: WebSockex.start(url, __MODULE__, observer, extra_headers: headers)

  @impl true
  def handle_connect(_connection, observer) do
    send(observer, {:partition_open, self()})
    {:ok, observer}
  end

  @impl true
  def handle_frame({kind, payload}, observer) do
    send(observer, {:partition_frame, self(), kind, payload})
    {:ok, observer}
  end

  @impl true
  def handle_disconnect(%{reason: reason}, observer) do
    send(observer, {:partition_closed, self(), reason})
    {:ok, observer}
  end
end
