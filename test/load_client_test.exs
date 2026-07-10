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
             "send_failures"
           ]) == %{
             "scenario" => "sustained",
             "requested_pairs" => 4,
             "requested_websockets" => 9,
             "connection_failures" => 0,
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
        "--duration",
        "0.2"
      ])

    result = Jason.decode!(output)

    assert status == 0
    assert result["requested_websockets"] == 4
    assert result["connection_successes"] == 4
    assert result["frames_received"] == 4
    assert result["connection_failures"] == 0
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
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
