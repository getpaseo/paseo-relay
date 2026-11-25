defmodule PaseoRelay.PlacementArrivalTest do
  use ExUnit.Case, async: false

  alias PaseoRelay.Placement

  # The listener binds 127.0.0.2. A same-host proxy connects from the bound
  # address; another node's reroute connects from a different address.
  @bound_ip {127, 0, 0, 2}
  @other_ip {127, 0, 0, 3}

  setup do
    reference = {:placement_arrival, System.unique_integer([:positive])}
    config = %{PaseoRelay.Config.defaults() | ip: @bound_ip}

    start_supervised!(
      {PaseoRelay.Listener, ref: reference, config: config, port: 0, acceptors: 2}
    )

    on_exit(&await_no_active_websockets/0)
    %{port: PaseoRelay.Listener.port(reference)}
  end

  test "a direct daemon control arrival records this node as home", %{port: port} do
    v2 = server_id("v2")
    v1 = server_id("v1")

    upgrade(port, "serverId=#{v2}&role=server&v=2", from: @bound_ip)
    upgrade(port, "serverId=#{v1}&role=server", from: @bound_ip)

    assert await_home(v2) == node()
    assert await_home(v1) == node()
  end

  test "clients, data connections and rerouted daemons never record a home", %{port: port} do
    client = server_id("client")
    data = server_id("data")
    rerouted_header = server_id("rerouted_header")
    rerouted_address = server_id("rerouted_address")
    control = server_id("control")

    upgrade(port, "serverId=#{client}&role=client&v=2", from: @bound_ip)
    upgrade(port, "serverId=#{data}&role=server&v=2&connectionId=c1", from: @bound_ip)

    upgrade(port, "serverId=#{rerouted_header}&role=server&v=2",
      from: @bound_ip,
      headers: ["x-paseo-rerouted: 1\r\n"]
    )

    upgrade(port, "serverId=#{rerouted_address}&role=server&v=2", from: @other_ip)
    # A later direct arrival is processed after the earlier ones.
    upgrade(port, "serverId=#{control}&role=server&v=2", from: @bound_ip)
    assert await_home(control) == node()

    assert Placement.home(client) == nil
    assert Placement.home(data) == nil
    assert Placement.home(rerouted_header) == nil
    assert Placement.home(rerouted_address) == nil
  end

  test "a client arrival does not replace a daemon's recorded home", %{port: port} do
    id = server_id("kept")
    upgrade(port, "serverId=#{id}&role=server&v=2", from: @bound_ip)
    assert await_home(id) == node()

    upgrade(port, "serverId=#{id}&role=client&v=2", from: @other_ip)
    assert Placement.home(id) == node()
  end

  defp server_id(label), do: "srv_arrival_#{label}_#{System.unique_integer([:positive])}"

  defp upgrade(port, query, options) do
    {:ok, socket} =
      :gen_tcp.connect(@bound_ip, port, [
        :binary,
        active: false,
        ip: Keyword.fetch!(options, :from)
      ])

    request = [
      "GET /ws?",
      query,
      " HTTP/1.1\r\nHost: relay.test\r\n",
      "Upgrade: websocket\r\nConnection: Upgrade\r\n",
      "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
      Keyword.get(options, :headers, []),
      "\r\n"
    ]

    :ok = :gen_tcp.send(socket, request)
    assert {:ok, "HTTP/1.1 101" <> _response} = :gen_tcp.recv(socket, 0, 2_000)
    close(socket)
  end

  defp close(socket) do
    :ok = :gen_tcp.send(socket, <<0x88, 0x80, :crypto.strong_rand_bytes(4)::binary>>)
    _ = :gen_tcp.recv(socket, 0, 2_000)
    :gen_tcp.close(socket)
  end

  defp await_no_active_websockets(deadline \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      PaseoRelay.Metrics.value(:active_websockets) == 0 -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("WebSockets stayed open")
      true -> await_no_active_websockets_after_sleep(deadline)
    end
  end

  defp await_no_active_websockets_after_sleep(deadline) do
    Process.sleep(10)
    await_no_active_websockets(deadline)
  end

  defp await_home(server_id, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    case Placement.home(server_id) do
      nil ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("no home recorded")
        Process.sleep(10)
        await_home(server_id, deadline)

      home ->
        home
    end
  end
end
