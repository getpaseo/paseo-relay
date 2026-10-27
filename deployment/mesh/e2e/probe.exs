defmodule PaseoRelay.MeshReplayE2E do
  import Bitwise

  @timeout 10_000

  def run(args) do
    {options, [], []} =
      OptionParser.parse(args,
        strict: [
          metrics: :string,
          hostname: :string,
          owner: :string,
          landing: :string
        ]
      )

    round_trip(options)
  end

  defp round_trip(options) do
    owner = {Keyword.fetch!(options, :owner), Keyword.get(options, :hostname)}
    landing = {Keyword.fetch!(options, :landing), Keyword.get(options, :hostname)}

    probe_id = unique_probe_id()
    server_id = "mesh-replay-#{probe_id}"
    connection_id = "connection-#{probe_id}"
    total_started_at = System.monotonic_time()

    before = reroutes(Keyword.fetch!(options, :metrics))

    result =
      with_socket(owner, server_id, "server", nil, fn control, started_at ->
        control = assert_control(control, "sync", nil)
        control_sync_ms = elapsed_ms(started_at)

        with_socket(landing, server_id, "client", connection_id, fn client, client_started_at ->
          assert_control(control, "connected", connection_id)
          client_attach_ms = elapsed_ms(client_started_at)

          with_socket(
            landing,
            server_id,
            "server",
            connection_id,
            fn data, data_started_at ->
              data_attach_ms = elapsed_ms(data_started_at)

              client_to_daemon_started_at = System.monotonic_time()
              :ok = send_frame(client, :text, "client-to-daemon")
              data = assert_frame(data, :text, "client-to-daemon")
              client_to_daemon_ms = elapsed_ms(client_to_daemon_started_at)

              daemon_to_client_started_at = System.monotonic_time()
              :ok = send_frame(data, :binary, <<0, 1, 2, 255>>)
              assert_frame(client, :binary, <<0, 1, 2, 255>>)
              daemon_to_client_ms = elapsed_ms(daemon_to_client_started_at)

              %{
                server_id: server_id,
                owner_endpoint: elem(owner, 0),
                landing_endpoint: elem(landing, 0),
                frames: 2,
                latency_ms: %{
                  control_sync: control_sync_ms,
                  client_attach: client_attach_ms,
                  data_attach: data_attach_ms,
                  client_to_daemon: client_to_daemon_ms,
                  daemon_to_client: daemon_to_client_ms,
                  total: elapsed_ms(total_started_at)
                }
              }
            end
          )
        end)
      end)

    after_count = reroutes(Keyword.fetch!(options, :metrics))
    unless after_count >= before + 2, do: raise("landing node did not record both reroutes")
    print(result |> Map.put(:status, "ok") |> Map.put(:reroutes, after_count - before))
  end

  defp with_socket(endpoint, server_id, role, connection_id, fun) do
    started_at = System.monotonic_time()
    socket = connect(endpoint, server_id, role, connection_id)

    try do
      fun.(socket, started_at)
    after
      close(socket)
    end
  end

  defp reroutes(endpoint) do
    {body, 0} =
      System.cmd("curl", [
        "--max-time",
        "10",
        "--fail",
        "--silent",
        "--show-error",
        endpoint <> "/metrics"
      ])

    [_, count] = Regex.run(~r/^paseo_relay_reroute_responses_total (\d+)$/m, to_string(body))
    String.to_integer(count)
  end

  defp elapsed_ms(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
    |> Kernel./(1_000)
    |> Float.round(2)
  end

  defp unique_probe_id do
    :crypto.strong_rand_bytes(16)
    |> Base.encode16(case: :lower)
  end

  defp connect(endpoint, server_id, role, connection_id) do
    try do
      do_connect(endpoint, server_id, role, connection_id)
    rescue
      error ->
        raise "failed to start #{role} websocket for #{server_id}: #{Exception.message(error)}"
    end
  end

  defp do_connect({endpoint, hostname}, server_id, role, connection_id) do
    uri = URI.parse(endpoint)
    hostname = hostname || uri.host
    transport = if uri.scheme == "wss", do: :ssl, else: :gen_tcp
    port = uri.port || if(transport == :ssl, do: 443, else: 80)

    query =
      [serverId: server_id, role: role, connectionId: connection_id, v: 2]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> URI.encode_query()

    path = String.trim_trailing(uri.path || "", "/") <> "/ws?#{query}"
    key = Base.encode64(:crypto.strong_rand_bytes(16))

    request = [
      "GET ",
      path,
      " HTTP/1.1\r\nHost: ",
      hostname,
      "\r\n",
      "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: ",
      key,
      "\r\n\r\n"
    ]

    case transport_connect(transport, uri.host, port, hostname) do
      {:ok, socket} ->
        try do
          :ok = transport.send(socket, request)
          {headers, rest} = recv_headers(transport, socket, "")
          unless headers =~ "HTTP/1.1 101", do: raise("WebSocket upgrade failed")
          %{transport: transport, socket: socket, buffer: rest}
        rescue
          error ->
            _ = transport.close(socket)
            reraise error, __STACKTRACE__
        end

      {:error, reason} ->
        raise "transport connect failed: #{inspect(reason)}"
    end
  end

  defp transport_connect(:gen_tcp, host, port, _hostname) do
    :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], @timeout)
  end

  defp transport_connect(:ssl, host, port, hostname) do
    {:ok, _} = Application.ensure_all_started(:ssl)

    options = [
      :binary,
      active: false,
      verify: :verify_peer,
      cacerts: :public_key.cacerts_get(),
      server_name_indication: String.to_charlist(hostname)
    ]

    :ssl.connect(String.to_charlist(host), port, options, @timeout)
  end

  defp recv_headers(transport, socket, bytes) do
    case :binary.split(bytes, "\r\n\r\n") do
      [headers, rest] ->
        {headers, rest}

      [_incomplete] ->
        {:ok, chunk} = transport.recv(socket, 0, @timeout)
        recv_headers(transport, socket, bytes <> chunk)
    end
  end

  defp send_frame(%{transport: transport, socket: socket}, kind, payload) do
    opcode = if kind == :text, do: 1, else: 2
    payload = IO.iodata_to_binary(payload)
    mask = :crypto.strong_rand_bytes(4)
    masked = mask(payload, mask)

    transport.send(socket, [<<0x80 ||| opcode>>, encoded_length(byte_size(payload)), mask, masked])
  end

  defp encoded_length(length) when length < 126, do: <<0x80 ||| length>>
  defp encoded_length(length) when length < 65_536, do: <<0xFE, length::16>>
  defp encoded_length(length), do: <<0xFF, length::64>>

  defp mask(payload, mask) do
    mask_bytes = :binary.bin_to_list(mask)

    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, index} -> bxor(byte, Enum.at(mask_bytes, rem(index, 4))) end)
    |> :binary.list_to_bin()
  end

  defp assert_control(socket, type, connection_id) do
    case recv_frame(socket) do
      {:text, payload, socket} ->
        message = Jason.decode!(payload)

        if message["type"] == type and
             (is_nil(connection_id) or message["connectionId"] == connection_id) do
          socket
        else
          assert_control(socket, type, connection_id)
        end

      {_kind, _payload, socket} ->
        assert_control(socket, type, connection_id)
    end
  end

  defp assert_frame(socket, kind, payload) do
    case recv_frame(socket) do
      {^kind, ^payload, socket} -> socket
      {_other_kind, _other_payload, socket} -> assert_frame(socket, kind, payload)
    end
  end

  defp recv_frame(socket) do
    {<<first, second>>, socket} = take(socket, 2)
    opcode = first &&& 0x0F
    masked? = (second &&& 0x80) != 0
    {length, socket} = frame_length(socket, second &&& 0x7F)
    {mask_key, socket} = if masked?, do: take(socket, 4), else: {nil, socket}
    {payload, socket} = take(socket, length)
    payload = if mask_key, do: mask(payload, mask_key), else: payload

    kind =
      case opcode do
        1 -> :text
        2 -> :binary
        8 -> :close
        9 -> :ping
        10 -> :pong
        _ -> :unknown
      end

    {kind, payload, socket}
  end

  defp frame_length(socket, 126) do
    {<<length::16>>, socket} = take(socket, 2)
    {length, socket}
  end

  defp frame_length(socket, 127) do
    {<<length::64>>, socket} = take(socket, 8)
    {length, socket}
  end

  defp frame_length(socket, length), do: {length, socket}

  defp take(%{buffer: buffer} = socket, count) when byte_size(buffer) >= count do
    <<wanted::binary-size(^count), rest::binary>> = buffer
    {wanted, %{socket | buffer: rest}}
  end

  defp take(%{transport: transport, socket: raw, buffer: buffer} = socket, count) do
    {:ok, chunk} = transport.recv(raw, 0, @timeout)
    take(%{socket | buffer: buffer <> chunk}, count)
  end

  defp close(%{transport: transport, socket: socket} = state) do
    try do
      close_payload = <<1_000::16>>
      mask_key = :crypto.strong_rand_bytes(4)
      masked = mask(close_payload, mask_key)
      :ok = transport.send(socket, [<<0x88>>, <<0x82>>, mask_key, masked])
      await_close(state)
    rescue
      _ -> :ok
    after
      _ = transport.close(socket)
    end
  end

  defp await_close(state) do
    case recv_frame(state) do
      {:close, _payload, _state} -> :ok
      {_kind, _payload, state} -> await_close(state)
    end
  rescue
    _ -> :ok
  end

  defp print(value), do: IO.puts(Jason.encode!(value))
end

PaseoRelay.MeshReplayE2E.run(System.argv())
