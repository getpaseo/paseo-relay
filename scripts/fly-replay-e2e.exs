defmodule PaseoRelay.FlyReplayE2E.Client do
  use WebSockex

  def start_link(url, owner, machine_id) do
    WebSockex.start_link(url, __MODULE__, owner,
      extra_headers: [{"Fly-Force-Instance-Id", machine_id}]
    )
  end

  @impl true
  def handle_connect(_connection, owner) do
    send(owner, {:open, self()})
    {:ok, owner}
  end

  @impl true
  def handle_frame({kind, payload}, owner) do
    send(owner, {:frame, self(), kind, payload})
    {:ok, owner}
  end

  @impl true
  def handle_disconnect(status, owner) do
    send(owner, {:closed, self(), status})
    {:ok, owner}
  end
end

defmodule PaseoRelay.FlyReplayE2E do
  alias PaseoRelay.FlyReplayE2E.Client

  def run(args) do
    {options, [], []} =
      OptionParser.parse(args,
        strict: [endpoint: :string, owner: :string, landing: :string]
      )

    endpoint = Keyword.fetch!(options, :endpoint)
    owner_machine = Keyword.fetch!(options, :owner)
    landing_machine = Keyword.fetch!(options, :landing)
    server_id = "fly-replay-#{System.unique_integer([:positive])}"
    connection_id = "connection-#{System.unique_integer([:positive])}"

    control = connect(endpoint, server_id, "server", nil, owner_machine)
    assert_control(control, "sync", nil)

    client = connect(endpoint, server_id, "client", connection_id, landing_machine)
    assert_control(control, "connected", connection_id)

    data = connect(endpoint, server_id, "server", connection_id, landing_machine)

    :ok = WebSockex.send_frame(client, {:text, "client-to-daemon"})
    assert_frame(data, :text, "client-to-daemon")

    :ok = WebSockex.send_frame(data, {:binary, <<0, 1, 2, 255>>})
    assert_frame(client, :binary, <<0, 1, 2, 255>>)

    Enum.each([control, client, data], &Process.exit(&1, :normal))

    IO.puts(
      Jason.encode!(%{
        status: "ok",
        server_id: server_id,
        owner_machine: owner_machine,
        forced_landing_machine: landing_machine,
        frames: 2
      })
    )
  end

  defp connect(endpoint, server_id, role, connection_id, machine_id) do
    query =
      [serverId: server_id, role: role, connectionId: connection_id, v: 2]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> URI.encode_query()

    {:ok, socket} = Client.start_link("#{endpoint}/ws?#{query}", self(), machine_id)
    await({:open, socket})
    socket
  end

  defp assert_control(socket, type, connection_id) do
    receive do
      {:frame, ^socket, :text, payload} ->
        message = Jason.decode!(payload)

        if message["type"] == type and
             (is_nil(connection_id) or message["connectionId"] == connection_id) do
          :ok
        else
          assert_control(socket, type, connection_id)
        end
    after
      10_000 -> raise "timed out waiting for control #{type}"
    end
  end

  defp assert_frame(socket, kind, payload) do
    await({:frame, socket, kind, payload})
  end

  defp await(expected) do
    receive do
      ^expected -> :ok
      {:closed, socket, status} -> raise "socket #{inspect(socket)} closed: #{inspect(status)}"
    after
      10_000 -> raise "timed out waiting for #{inspect(expected)}"
    end
  end
end

PaseoRelay.FlyReplayE2E.run(System.argv())
