defmodule PaseoRelay.RelayProtocolTest do
  use ExUnit.Case, async: false

  setup do
    assert PaseoRelay.Metrics.value(:active_websockets) == 0
    :ok
  end

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

    def handle_cast(:close, owner), do: {:close, owner}
  end

  test "v1 server and client forward ordered text and binary frames" do
    port = start_relay()

    {:ok, daemon} = connect(v1_url(port, "server"))
    assert_receive {:relay_open, ^daemon}

    {:ok, client} = connect(v1_url(port, "client"))
    assert_receive {:relay_open, ^client}

    :ok = WebSockex.send_frame(client, {:text, "one"})
    :ok = WebSockex.send_frame(client, {:binary, <<0, 255, 1>>})

    assert_receive {:relay_frame, ^daemon, :text, "one"}
    assert_receive {:relay_frame, ^daemon, :binary, <<0, 255, 1>>}

    close_clients([daemon, client])
  end

  test "v2 control pairs clients with data sockets and flushes buffered frames in order" do
    port = start_relay()

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

    close_clients([control, data, client])
  end

  test "v2 control answers the legacy JSON ping with a JSON pong" do
    port = start_relay()

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})
    frames = PaseoRelay.Metrics.value(:frames_forwarded)
    bytes = PaseoRelay.Metrics.value(:bytes_forwarded)

    :ok = WebSockex.send_frame(control, {:text, ~s({"type":"ping"})})

    pong = assert_control_type(control, "pong")
    assert PaseoRelay.Metrics.value(:frames_forwarded) == frames + 1
    assert PaseoRelay.Metrics.value(:bytes_forwarded) == bytes + byte_size(pong)

    close_clients([control])
  end

  @tag timeout: 8_000
  test "v2 control fails closed when its Owner stalls during a ping" do
    port = start_relay()

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})
    owner = PaseoRelay.Ownership.owner_pid(v2_server_id(port))
    owner_ref = Process.monitor(owner)
    :ok = :sys.suspend(owner)

    on_exit(fn ->
      if Process.alive?(owner), do: :sys.resume(owner)
    end)

    :ok = WebSockex.send_frame(control, {:text, ~s({"type":"ping"})})
    assert_receive {:relay_closed, ^control, {:remote, 1013, "Delivery unavailable"}}, 7_000
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}, 1_000
    close_clients([control])
  end

  test "v2 resets an unresponsive control after nudging it to attach client data" do
    port = start_relay()

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})

    {:ok, client} = connect(v2_url(port, "client", "clt_watchdog"))
    assert_receive {:relay_open, ^client}
    assert_control(control, %{"type" => "connected", "connectionId" => "clt_watchdog"})
    assert_control(control, %{"type" => "sync", "connectionIds" => ["clt_watchdog"]}, 11_000)
    assert_receive {:relay_closed, ^control, {:remote, 1011, "Control unresponsive"}}, 6_000

    close_clients([control, client])
  end

  test "v2 payload delivery has no node-wide registry process" do
    port = start_relay()

    {:ok, client} = connect(v2_url(port, "client", "clt_registry_crash"))
    assert_receive {:relay_open, ^client}

    {:ok, data} = connect(v2_url(port, "server", "clt_registry_crash"))
    assert_receive {:relay_open, ^data}

    assert Process.whereis(PaseoRelay.Registry) == nil

    :ok = WebSockex.send_frame(client, {:text, "owner-routed"})
    assert_receive {:relay_frame, ^data, :text, "owner-routed"}

    close_clients([client, data])
  end

  test "v2 sockets fail closed when their distributed session owner exits" do
    port = start_relay()

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})

    Process.exit(PaseoRelay.Ownership.owner_pid(v2_server_id(port)), :session_conflict)

    assert_receive {:relay_closed, ^control, {:remote, 1012, "Session owner moved"}}, 1_000
    close_clients([control])
  end

  test "v2 socket initialization fails closed when its reserved owner has moved" do
    server_id = "srv_owner_moved_#{System.unique_integer([:positive])}"
    budget_namespace = {:owner_moved, System.unique_integer([:positive])}

    {:ok, connection} =
      PaseoRelay.Connection.from_query(%{"serverId" => server_id, "role" => "server", "v" => "2"})

    {:local, owner, reservation} = PaseoRelay.Ownership.route(server_id, "local")
    owner_down = Process.monitor(owner)
    Process.exit(owner, :session_conflict)
    assert_receive {:DOWN, ^owner_down, :process, ^owner, :session_conflict}

    assert {:ok, admission} =
             PaseoRelay.Capacity.admit_connection(
               budget_namespace,
               1,
               PaseoRelay.Config.defaults().capacity_mutation_timeout_ms
             )

    state = %{
      admission: admission,
      config: &PaseoRelay.Config.defaults/0,
      connection: connection,
      owner: owner,
      reservation: reservation
    }

    assert {[{:close, 1012, "Session expired"}], failed_state} =
             PaseoRelay.Socket.websocket_init(state)

    PaseoRelay.Capacity.release_connection(failed_state.admission.token)
    assert PaseoRelay.Capacity.active_connections(budget_namespace) == 0
  end

  @tag timeout: 75_000
  test "an idle websocket remains open past the adapter default timeout" do
    port = start_relay()

    {:ok, socket} = connect(v1_url(port, "server"))
    assert_receive {:relay_open, ^socket}

    refute_receive {:relay_closed, ^socket, _}, 61_000

    close_clients([socket])
  end

  test "v2 closes data with the last client and tells control it disconnected" do
    port = start_relay()

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})

    {:ok, client} = connect(v2_url(port, "client", "clt_closes"))
    assert_receive {:relay_open, ^client}
    assert_control(control, %{"type" => "connected", "connectionId" => "clt_closes"})

    {:ok, data} = connect(v2_url(port, "server", "clt_closes"))
    assert_receive {:relay_open, ^data}
    :ok = WebSockex.send_frame(data, {:text, "data-attached"})
    assert_receive {:relay_frame, ^client, :text, "data-attached"}

    close_client(client)

    assert_receive {:relay_closed, ^data, {:remote, 1001, "Client disconnected"}}
    assert_control(control, %{"type" => "disconnected", "connectionId" => "clt_closes"})

    close_clients([control, client, data])
  end

  test "v2 replaces duplicate daemon data without disconnecting the client route" do
    port = start_relay()

    {:ok, client} = connect(v2_url(port, "client", "clt_replace"))
    assert_receive {:relay_open, ^client}

    {:ok, original} = connect(v2_url(port, "server", "clt_replace"))
    assert_receive {:relay_open, ^original}

    {:ok, replacement} = connect(v2_url(port, "server", "clt_replace"))
    assert_receive {:relay_open, ^replacement}
    assert_receive {:relay_closed, _, {:remote, 1008, "Replaced by new connection"}}

    close_clients([client, original, replacement])
  end

  test "v1 validates every current handshake retry before forwarding" do
    port = start_relay()

    {:ok, daemon} = connect(v1_url(port, "server"))
    assert_receive {:relay_open, ^daemon}

    {:ok, client} = connect(v1_url(port, "client"))
    assert_receive {:relay_open, ^client}

    accepted = handshake_metric(:accepted, 1, :e2ee_hello)
    rejected = handshake_metric(:rejected, 1, :e2ee_hello)
    hello = handshake("e2ee_hello", valid_public_key())
    invalid_key = handshake("e2ee_hello", <<0::256>>)

    :ok = WebSockex.send_frame(client, {:text, hello})
    assert_receive {:relay_frame, ^daemon, :text, ^hello}

    :ok = WebSockex.send_frame(client, {:text, hello})
    assert_receive {:relay_frame, ^daemon, :text, ^hello}

    :ok = WebSockex.send_frame(client, {:text, invalid_key})
    assert_receive {:relay_closed, ^client, {:remote, 1008, "Invalid handshake key"}}
    refute_receive {:relay_frame, ^daemon, :text, ^invalid_key}, 100
    assert handshake_metric(:accepted, 1, :e2ee_hello) == accepted + 2
    assert handshake_metric(:rejected, 1, :e2ee_hello) == rejected + 1

    close_clients([daemon, client])
  end

  test "v2 validates legacy binary handshakes without breaking pipelined ciphertext" do
    port = start_relay()

    {:ok, control} = connect(v2_url(port, "server"))
    assert_receive {:relay_open, ^control}
    assert_control(control, %{"type" => "sync", "connectionIds" => []})

    {:ok, client} = connect(v2_url(port, "client", "clt_handshake_validation"))
    assert_receive {:relay_open, ^client}

    assert_control(control, %{
      "type" => "connected",
      "connectionId" => "clt_handshake_validation"
    })

    {:ok, data} = connect(v2_url(port, "server", "clt_handshake_validation"))
    assert_receive {:relay_open, ^data}

    accepted = handshake_metric(:accepted, 2, :hello)
    rejected = handshake_metric(:rejected, 2, :hello)
    hello = handshake("hello", valid_public_key())
    ciphertext = :binary.copy(<<0xA5>>, 48)

    :ok = WebSockex.send_frame(client, {:binary, hello})
    :ok = WebSockex.send_frame(client, {:binary, ciphertext})

    assert_receive {:relay_frame, ^data, :binary, ^hello}
    assert_receive {:relay_frame, ^data, :binary, ^ciphertext}

    invalid_key =
      handshake(
        "hello",
        Base.decode16!("E0EB7A7C3B41B8AE1656E3FAF19FC46ADA098DEB9C32B1FD866205165F49B800")
      )

    :ok = WebSockex.send_frame(client, {:binary, invalid_key})

    assert_receive {:relay_closed, ^client, {:remote, 1008, "Invalid handshake key"}}
    refute_receive {:relay_frame, ^data, :binary, ^invalid_key}, 100
    assert handshake_metric(:accepted, 2, :hello) == accepted + 1
    assert handshake_metric(:rejected, 2, :hello) == rejected + 1

    close_clients([control, data, client])
  end

  defp v1_url(port, role) do
    "ws://127.0.0.1:#{port}/ws?serverId=srv_v1&role=#{role}"
  end

  defp connect(url) do
    {:ok, client} = RelayClient.start_link(url, self())
    Process.unlink(client)
    {:ok, client}
  end

  defp close_clients(clients) do
    clients
    |> Enum.uniq()
    |> Enum.each(&close_client/1)

    deadline = System.monotonic_time(:millisecond) + 5_000
    await_no_active_websockets(deadline)
  end

  defp close_client(client) do
    if Process.alive?(client) do
      reference = Process.monitor(client)
      WebSockex.cast(client, :close)

      receive do
        {:DOWN, ^reference, :process, ^client, _reason} -> :ok
      after
        5_000 -> flunk("client #{inspect(client)} did not close its WebSocket synchronously")
      end
    end
  end

  defp await_no_active_websockets(deadline) do
    if PaseoRelay.Metrics.value(:active_websockets) == 0 do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("relay WebSockets did not return to zero")
      end

      Process.sleep(10)
      await_no_active_websockets(deadline)
    end
  end

  defp v2_url(port, role, connection_id \\ nil) do
    query = if connection_id, do: "&connectionId=#{connection_id}", else: ""
    "ws://127.0.0.1:#{port}/ws?serverId=#{v2_server_id(port)}&role=#{role}&v=2#{query}"
  end

  defp v2_server_id(port) do
    key = {__MODULE__, :server_id, port}

    Process.get(key) ||
      tap("srv_v2_#{port}_#{System.unique_integer([:positive])}", &Process.put(key, &1))
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp start_relay do
    port = available_port()
    reference = {:relay_protocol, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: port,
       acceptors: 4,
       max_connections: 1_000}
    )

    port
  end

  defp assert_control(client, message, timeout \\ 100) do
    assert_receive {:relay_frame, ^client, :text, payload}, timeout
    assert Jason.decode!(payload) == message
  end

  defp assert_control_type(client, type) do
    assert_receive {:relay_frame, ^client, :text, payload}
    assert %{"type" => ^type, "ts" => ts} = Jason.decode!(payload)
    assert is_integer(ts)
    payload
  end

  defp valid_public_key do
    {public_key, _private_key} = :crypto.generate_key(:ecdh, :x25519)
    public_key
  end

  defp handshake(type, public_key) do
    Jason.encode!(%{type: type, key: Base.encode64(public_key), capabilities: %{}})
  end

  defp handshake_metric(outcome, version, type) do
    PaseoRelay.Metrics.value({:handshake, outcome, version, type})
  end
end
