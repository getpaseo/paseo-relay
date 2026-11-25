defmodule PaseoRelay.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias PaseoRelay.Placement
  alias PaseoRelay.RuntimeConfig

  @watermark 64 * 1024 * 1024 * 1024

  setup do
    stored = Application.get_env(:paseo_relay, :runtime)
    live = RuntimeConfig.get()

    on_exit(fn ->
      {:ok, _settings} = RuntimeConfig.put(live)
      Application.put_env(:paseo_relay, :runtime, stored)
    end)
  end

  test "invalid settings are rejected and nothing changes" do
    before = RuntimeConfig.get()

    assert RuntimeConfig.put(%{memory_watermark_bytes: 1}) ==
             {:error,
              "memory_watermark_bytes must be 0 or an integer between 268435456 and 68719476736"}

    assert RuntimeConfig.put(%{heal_rate_per_sec: 0}) ==
             {:error, "heal_rate_per_sec must be an integer between 1 and 500"}

    assert RuntimeConfig.put(heal_rate_per_sec: 501) ==
             {:error, "heal_rate_per_sec must be an integer between 1 and 500"}

    assert RuntimeConfig.put(%{heal_delay_ms: -1}) ==
             {:error, "heal_delay_ms must be an integer between 0 and 86400000"}

    assert RuntimeConfig.put(%{heal_enabled: "false"}) ==
             {:error, "heal_enabled must be true or false"}

    assert RuntimeConfig.put(%{port: 4001}) == {:error, ":port is not a live setting"}

    # One invalid value rejects the whole change.
    assert {:error, _message} = RuntimeConfig.put(%{heal_rate_per_sec: 10, heal_delay_ms: -1})
    assert RuntimeConfig.get() == before
    assert Placement.settings() == Map.delete(before, :memory_watermark_bytes)
  end

  test "valid settings apply live, keep sockets open, show in metrics and survive restarts" do
    socket = open_daemon("srv_runtime_config_#{System.unique_integer([:positive])}")

    settings = %{
      memory_watermark_bytes: @watermark,
      heal_enabled: false,
      heal_delay_ms: 1_234,
      heal_rate_per_sec: 7
    }

    assert RuntimeConfig.put(settings) == {:ok, settings}
    assert RuntimeConfig.get() == settings
    assert metric("memory_watermark_bytes") == @watermark
    assert Placement.settings() == Map.delete(settings, :memory_watermark_bytes)

    # The daemon socket that was open before the change still answers.
    :ok = :gen_tcp.send(socket, masked_text(~s({"type":"ping"})))
    assert {:ok, <<0x81, _length, pong::binary>>} = :gen_tcp.recv(socket, 0, 2_000)
    assert pong =~ ~s("type":"pong")
    close(socket)

    restart(PaseoRelay.Capacity)
    assert metric("memory_watermark_bytes") == @watermark

    restart(PaseoRelay.Placement.Server)
    assert Placement.settings() == Map.delete(settings, :memory_watermark_bytes)
  end

  test "a change Placement cannot apply is rejected and rolled back everywhere" do
    before = RuntimeConfig.get()
    placement = Process.whereis(PaseoRelay.Placement.Server)
    :ok = :sys.suspend(placement)

    try do
      assert RuntimeConfig.put(%{memory_watermark_bytes: @watermark, heal_rate_per_sec: 9}) ==
               {:error, "Placement is unavailable; nothing changed"}

      assert RuntimeConfig.get() == before
      assert metric("memory_watermark_bytes") == before.memory_watermark_bytes
    after
      :sys.resume(placement)
    end

    # The change that arrived late does not survive the rollback.
    assert Placement.settings() == Map.delete(before, :memory_watermark_bytes)
  end

  test "a change Capacity cannot apply is rejected and nothing changes" do
    before = RuntimeConfig.get()
    stored = PaseoRelay.Config.normalize(Application.get_env(:paseo_relay, :runtime))
    Application.put_env(:paseo_relay, :runtime, %{stored | capacity_mutation_timeout_ms: 200})
    capacity = Process.whereis(PaseoRelay.Capacity)
    :ok = :sys.suspend(capacity)

    assert RuntimeConfig.put(%{memory_watermark_bytes: @watermark, heal_rate_per_sec: 9}) ==
             {:error, "Capacity is unavailable; nothing changed"}

    # Capacity fails closed and restarts with the stored value.
    await_replacement(PaseoRelay.Capacity, capacity, System.monotonic_time(:millisecond) + 5_000)
    assert RuntimeConfig.get() == before
    assert metric("memory_watermark_bytes") == before.memory_watermark_bytes
    assert Placement.settings() == Map.delete(before, :memory_watermark_bytes)
  end

  defp metric(name) do
    {200, _content_type, body} = PaseoRelay.Operations.response("/metrics")
    [_, value] = Regex.run(~r/^paseo_relay_#{name} (\d+)$/m, body)
    String.to_integer(value)
  end

  defp restart(name) do
    process = Process.whereis(name)
    reference = Process.monitor(process)
    Process.exit(process, :kill)
    assert_receive {:DOWN, ^reference, :process, ^process, :killed}
    await_replacement(name, process, System.monotonic_time(:millisecond) + 5_000)
  end

  defp await_replacement(name, previous, deadline) do
    case Process.whereis(name) do
      replacement when is_pid(replacement) and replacement != previous ->
        # The listener restarts after Capacity, so wait until it serves again.
        await_listener(deadline)

      _missing ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("#{name} did not restart")
        Process.sleep(10)
        await_replacement(name, previous, deadline)
    end
  end

  defp await_listener(deadline) do
    case :ranch.get_port(PaseoRelay.Listener) do
      port when is_integer(port) ->
        :ok

      _missing ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("listener did not restart")
        Process.sleep(10)
        await_listener(deadline)
    end
  catch
    _kind, _reason ->
      Process.sleep(10)
      await_listener(deadline)
  end

  defp open_daemon(server_id) do
    port = PaseoRelay.Listener.port(PaseoRelay.Listener)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        socket,
        "GET /ws?serverId=#{server_id}&role=server&v=2 HTTP/1.1\r\nHost: relay.test\r\n" <>
          "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n" <>
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
      )

    assert {:ok, "HTTP/1.1 101" <> _response} = :gen_tcp.recv(socket, 0, 2_000)
    # The Owner sends its initial sync notification.
    assert {:ok, <<0x81, _length, _sync::binary>>} = :gen_tcp.recv(socket, 0, 2_000)
    socket
  end

  defp masked_text(payload) do
    mask = :crypto.strong_rand_bytes(4)
    <<0x81, 0x80 + byte_size(payload), mask::binary, xor(payload, mask)::binary>>
  end

  defp xor(payload, <<a, b, c, d>>) do
    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, index} -> Bitwise.bxor(byte, elem({a, b, c, d}, rem(index, 4))) end)
    |> :erlang.list_to_binary()
  end

  defp close(socket) do
    :ok = :gen_tcp.send(socket, <<0x88, 0x80, :crypto.strong_rand_bytes(4)::binary>>)
    _ = :gen_tcp.recv(socket, 0, 2_000)
    :gen_tcp.close(socket)
    await_no_active_websockets(System.monotonic_time(:millisecond) + 5_000)
  end

  defp await_no_active_websockets(deadline) do
    cond do
      PaseoRelay.Metrics.value(:active_websockets) == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("WebSockets stayed open")

      true ->
        Process.sleep(10)
        await_no_active_websockets(deadline)
    end
  end
end
