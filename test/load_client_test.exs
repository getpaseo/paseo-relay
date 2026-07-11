defmodule PaseoRelay.LoadClientTest.DelayedRelay do
  @behaviour Plug

  @impl Plug
  def init(options), do: options

  @impl Plug
  def call(conn, options) do
    conn = Plug.Conn.fetch_query_params(conn)

    if conn.query_params["role"] == "server" && conn.query_params["connectionId"] do
      Process.sleep(Keyword.fetch!(options, :delay_ms))
    end

    PaseoRelay.Router.call(conn, [])
  end
end

defmodule PaseoRelay.LoadClientTest do
  use ExUnit.Case, async: false

  setup context do
    if context[:relay] do
      port = available_port()
      relay_pid = start_relay(port)

      on_exit(fn ->
        stop_relay(relay_pid)
      end)

      %{port: port, relay_pid: relay_pid}
    else
      %{}
    end
  end

  test "the black-box client documents generic v2 websocket roles" do
    {output, status} = System.cmd("node", ["scripts/relay-load.mjs", "--help"])

    assert status == 0
    assert output =~ "serverId"
    assert output =~ "connectionId"
    assert output =~ "--endpoints"
    assert output =~ "--cleanup-grace"
  end

  test "a failed setup closes a sibling socket that opens later" do
    relay_port = available_port()
    unavailable_port = available_port()

    relays = [
      start_endpoint(PaseoRelay.LoadClientTest.DelayedRelay, relay_port, delay_ms: 500),
      start_endpoint(PaseoRelay.Operations, unavailable_port, [])
    ]

    on_exit(fn -> Enum.each(relays, &Process.exit(&1, :shutdown)) end)

    {output, status} =
      run_load(
        [
          "--endpoints",
          "ws://127.0.0.1:#{relay_port}/ws,ws://127.0.0.1:#{unavailable_port}/ws",
          "--server-id",
          "delayed-failed-setup",
          "--pairs",
          "1",
          "--duration",
          "0",
          "--cleanup-grace",
          "2"
        ],
        3_000
      )

    result = Jason.decode!(output)

    assert status == 1
    assert result["connection_failures"] > 0
    assert result["error"] =~ "non-101 status code"
    assert result["cleanup_timeouts"] == 0
    assert metric_value(request(relay_port, "/metrics"), "active_websockets") == 0
  end

  @tag :relay
  test "a ramped sustained run relays frames and finishes without cleanup failures", %{
    port: port,
    relay_pid: relay_pid
  } do
    {output, status} =
      System.cmd("node", [
        "scripts/relay-load.mjs",
        "--endpoints",
        "ws://127.0.0.1:#{port}/ws",
        "--pairs",
        "4",
        "--batch-size",
        "2",
        "--ramp-ms",
        "5",
        "--scenario",
        "sustained",
        "--duration",
        "1",
        "--rate",
        "20"
      ])

    result = Jason.decode!(output)

    assert status == 0

    assert Map.take(result, [
             "scenario",
             "requested_pairs",
             "requested_websockets",
             "connection_failures",
             "cleanup_timeouts",
             "send_failures"
           ]) == %{
             "scenario" => "sustained",
             "requested_pairs" => 4,
             "requested_websockets" => 9,
             "connection_failures" => 0,
             "cleanup_timeouts" => 0,
             "send_failures" => 0
           }

    assert result["frames_received"] > 0
    stop_relay(relay_pid)
  end

  @tag :relay
  test "a sharded run can omit the shared control socket", %{port: port} do
    {output, status} =
      System.cmd("node", [
        "scripts/relay-load.mjs",
        "--endpoints",
        "ws://127.0.0.1:#{port}/ws",
        "--server-id",
        "shared-load-server",
        "--connection-prefix",
        "shard-a",
        "--no-control",
        "--pairs",
        "2",
        "--scenario",
        "burst",
        "--burst",
        "1",
        "--keepalive",
        "0.05",
        "--duration",
        "0.2"
      ])

    result = Jason.decode!(output)

    assert status == 0
    assert result["requested_websockets"] == 4
    assert result["connection_successes"] == 4
    assert result["frames_received"] >= 4
    assert is_integer(result["keepalive_frames_sent"])
    assert result["keepalive_frames_sent"] > 0
    assert result["connection_failures"] == 0
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp start_endpoint(module, port, options) do
    {:ok, endpoint} = Bandit.start_link(plug: {module, options}, port: port)
    Process.unlink(endpoint)
    endpoint
  end

  defp run_load(arguments, timeout) do
    command =
      Port.open({:spawn_executable, System.find_executable("node")}, [
        :binary,
        :exit_status,
        args: ["scripts/relay-load.mjs" | arguments]
      ])

    await_command(command, "", System.monotonic_time(:millisecond) + timeout)
  end

  defp await_command(command, output, deadline) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))

    receive do
      {^command, {:data, data}} -> await_command(command, output <> data, deadline)
      {^command, {:exit_status, status}} -> {output, status}
    after
      remaining ->
        {:os_pid, pid} = Port.info(command, :os_pid)
        System.cmd("kill", ["-KILL", Integer.to_string(pid)])
        flunk("load client did not exit within #{remaining}ms after setup failed")
    end
  end

  defp request(port, path) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        socket,
        "GET #{path} HTTP/1.1\r\nHost: relay.test\r\nConnection: close\r\n\r\n"
      )

    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    :gen_tcp.close(socket)
    response
  end

  defp metric_value(metrics, name) do
    [_, value] = Regex.run(~r/paseo_relay_#{name} (\d+)/, metrics)
    String.to_integer(value)
  end

  defp start_relay(port) do
    start =
      """
      Application.put_env(:paseo_relay, :operations, [host: \"127.0.0.1\", ip: {127, 0, 0, 1}, port: #{port}, drain: false]);
      Application.ensure_all_started(:paseo_relay)
      """

    relay =
      Port.open({:spawn_executable, System.find_executable("mix")}, [
        :binary,
        :exit_status,
        args: ["run", "--no-start", "--no-halt", "-e", start],
        env: [{~c"MIX_ENV", ~c"test"}]
      ])

    {:os_pid, relay_pid} = Port.info(relay, :os_pid)
    wait_for_listener(port, 50)
    relay_pid
  end

  defp stop_relay(pid) do
    case System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_, 0} ->
        case System.cmd("kill", ["-TERM", Integer.to_string(pid)], stderr_to_stdout: true) do
          {_, 0} -> wait_for_process_exit(pid, 20)
          {_, 1} -> :ok
        end

      {_, 1} ->
        :ok
    end
  end

  defp wait_for_listener(_port, 0), do: flunk("relay did not start")

  defp wait_for_listener(port, attempts) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary], 100) do
      {:ok, socket} ->
        :gen_tcp.close(socket)

      {:error, :econnrefused} ->
        Process.sleep(50)
        wait_for_listener(port, attempts - 1)
    end
  end

  defp wait_for_process_exit(_pid, 0), do: flunk("relay did not stop")

  defp wait_for_process_exit(pid, attempts) do
    case System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_, 1} ->
        :ok

      {_, 0} ->
        Process.sleep(50)
        wait_for_process_exit(pid, attempts - 1)
    end
  end
end
