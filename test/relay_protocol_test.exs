defmodule PaseoRelay.RelayProtocolTest do
  use ExUnit.Case, async: false

  defmodule RelayClient do
    use WebSockex

    def start_link(url, owner), do: WebSockex.start_link(url, __MODULE__, owner)

    def handle_connect(_connection, owner) do
      send(owner, {:relay_open, self()})
      {:ok, owner}
    end

    def handle_frame({kind, payload}, owner) do
      send(owner, {:relay_frame, self(), kind, payload})
      {:ok, owner}
    end

    def handle_disconnect(%{reason: reason}, owner) do
      send(owner, {:relay_closed, self(), reason})
      {:ok, owner}
    end
  end

  test "v1 server and client forward ordered text and binary frames" do
    port = available_port()
    {:ok, relay} = Bandit.start_link(plug: PaseoRelay.Router, port: port)
    Process.unlink(relay)
    on_exit(fn -> Process.exit(relay, :shutdown) end)

    {:ok, daemon} = connect(v1_url(port, "server"))
    assert_receive {:relay_open, ^daemon}

    {:ok, client} = connect(v1_url(port, "client"))
    assert_receive {:relay_open, ^client}

    :ok = WebSockex.send_frame(client, {:text, "one"})
    :ok = WebSockex.send_frame(client, {:binary, <<0, 255, 1>>})

    assert_receive {:relay_frame, ^daemon, :text, "one"}
    assert_receive {:relay_frame, ^daemon, :binary, <<0, 255, 1>>}

    GenServer.stop(daemon)
    GenServer.stop(client)
  end

  test "v2 control pairs clients with data sockets and flushes buffered frames in order" do
    port = available_port()
    {:ok, relay} = Bandit.start_link(plug: PaseoRelay.Router, port: port)
    Process.unlink(relay)
    on_exit(fn -> Process.exit(relay, :shutdown) end)

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})

    {:ok, client} = connect(v2_url(port, "client", "clt_v2"))
    assert_receive {:relay_open, ^client}
    assert_control(control, %{"type" => "connected", "connectionId" => "clt_v2"})

    :ok = WebSockex.send_frame(client, {:text, "before-data"})
    :ok = WebSockex.send_frame(client, {:binary, <<2, 3, 5>>})

    {:ok, data} = connect(v2_url(port, "server", "clt_v2"))
    assert_receive {:relay_open, ^data}
    assert_receive {:relay_frame, ^data, :text, "before-data"}
    assert_receive {:relay_frame, ^data, :binary, <<2, 3, 5>>}

    :ok = WebSockex.send_frame(data, {:text, "from-daemon"})
    assert_receive {:relay_frame, ^client, :text, "from-daemon"}

    GenServer.stop(control)
    GenServer.stop(data)
    GenServer.stop(client)
  end

  test "v2 closes data with the last client and tells control it disconnected" do
    port = available_port()
    {:ok, relay} = Bandit.start_link(plug: PaseoRelay.Router, port: port)
    Process.unlink(relay)
    on_exit(fn -> Process.exit(relay, :shutdown) end)

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})

    {:ok, client} = connect(v2_url(port, "client", "clt_closes"))
    assert_receive {:relay_open, ^client}
    assert_control(control, %{"type" => "connected", "connectionId" => "clt_closes"})

    {:ok, data} = connect(v2_url(port, "server", "clt_closes"))
    assert_receive {:relay_open, ^data}

    GenServer.stop(client)

    assert_receive {:relay_closed, ^data, {:remote, 1001, "Client disconnected"}}
    assert_control(control, %{"type" => "disconnected", "connectionId" => "clt_closes"})

    GenServer.stop(control)
  end

  test "v2 replaces duplicate daemon data without disconnecting the client route" do
    port = available_port()
    {:ok, relay} = Bandit.start_link(plug: PaseoRelay.Router, port: port)
    Process.unlink(relay)
    on_exit(fn -> Process.exit(relay, :shutdown) end)

    {:ok, client} = connect(v2_url(port, "client", "clt_replace"))
    assert_receive {:relay_open, ^client}

    {:ok, original} = connect(v2_url(port, "server", "clt_replace"))
    assert_receive {:relay_open, ^original}

    {:ok, replacement} = connect(v2_url(port, "server", "clt_replace"))
    assert_receive {:relay_open, ^replacement}
    assert_receive {:relay_closed, _, {:remote, 1008, "Replaced by new connection"}}

    GenServer.stop(client)
  end

  defp v1_url(port, role) do
    "ws://127.0.0.1:#{port}/ws?serverId=srv_v1&role=#{role}"
  end

  defp connect(url) do
    {:ok, client} = RelayClient.start_link(url, self())
    Process.unlink(client)
    {:ok, client}
  end

  defp v2_url(port, role, connection_id \\ nil) do
    query = if connection_id, do: "&connectionId=#{connection_id}", else: ""
    "ws://127.0.0.1:#{port}/ws?serverId=srv_v2_#{port}&role=#{role}&v=2#{query}"
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp assert_control(client, message) do
    assert_receive {:relay_frame, ^client, :text, payload}
    assert Jason.decode!(payload) == message
  end
end
