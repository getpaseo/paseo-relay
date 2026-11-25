defmodule PaseoRelay.Listener do
  @moduledoc """
  The Cowboy listener of the relay.

  Without the `config` option the listener follows the live settings: each
  request reads `PaseoRelay.RuntimeConfig`, a restart opens the listen socket
  with the current settings, and `reconfigure/3` applies a change to the
  running listener and its open sockets.
  """

  alias PaseoRelay.Config

  # The socket options that a setting can change on an open socket. `recbuf` is
  # also one, but only when the setting is a number of bytes.
  @live_socket_options [:buffer, :recbuf, :send_timeout]

  def child_spec(options) do
    %{
      id: Keyword.fetch!(options, :ref),
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  # Builds the Ranch options at each start, so a restarted listener opens its
  # listen socket with the current settings.
  def start_link(options) do
    reference = Keyword.fetch!(options, :ref)
    config = config_reader(options)

    %{start: {module, function, arguments}} =
      :ranch.child_spec(
        reference,
        :ranch_tcp,
        transport_options(options, config.()),
        :cowboy_clear,
        protocol_options(options, config, reference)
      )

    apply(module, function, arguments)
  end

  def port(reference), do: :ranch.get_port(reference)

  @doc """
  Applies the changed settings to the listener `reference`: the connection
  ceiling of each acceptor, the HTTP idle timeout of new connections, and the
  socket options of the listen socket and of every open socket. A listener that
  is not running takes the settings when it starts.
  """
  @spec reconfigure(:ranch.ref(), Config.t(), Config.t()) :: :ok
  def reconfigure(reference, %Config{} = previous, %Config{} = config) do
    if listener_settings(previous) != listener_settings(config) do
      transport = :ranch.get_transport_options(reference)
      socket_options = live_socket_options(config)

      :ok =
        :ranch.set_transport_options(reference, %{
          transport
          | max_connections: config.connections_per_acceptor,
            socket_opts:
              transport.socket_opts
              |> Keyword.drop(@live_socket_options)
              |> Keyword.merge(socket_options)
        })

      :ok =
        :ranch.set_protocol_options(reference, %{
          :ranch.get_protocol_options(reference)
          | idle_timeout: config.http_idle_timeout_ms
        })

      for socket <- sockets(reference), do: :inet.setopts(socket, socket_options)
    end

    :ok
  rescue
    # Ranch does not know a listener that is stopped or restarting.
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  @doc false
  # The options of the HTTP and WebSocket handlers. `config` returns the
  # settings for the current request. `connection_budget` returns the Capacity
  # namespace and its ceiling for open WebSockets.
  def handler_options(config, namespace \\ __MODULE__, max_websockets \\ nil) do
    %{
      config: config,
      connection_budget: fn ->
        {namespace, max_websockets || Config.max_websockets(config.())}
      end
    }
  end

  defp config_reader(options) do
    case Keyword.fetch(options, :config) do
      {:ok, config} ->
        config = Config.normalize(config)
        fn -> config end

      :error ->
        &PaseoRelay.RuntimeConfig.current/0
    end
  end

  defp transport_options(options, config) do
    acceptors = Keyword.get(options, :acceptors, config.acceptors)

    %{
      num_acceptors: acceptors,
      num_conns_sups: Keyword.get(options, :connection_supervisors, acceptors),
      max_connections:
        Keyword.get(
          options,
          :connections_per_supervisor,
          Keyword.get(options, :max_connections, config.connections_per_acceptor)
        ),
      socket_opts: socket_options(options, config)
    }
  end

  defp socket_options(options, config) do
    [
      ip: Keyword.get(options, :ip, config.ip),
      port: Keyword.get(options, :port, config.port),
      nodelay: true,
      send_timeout_close: true
    ]
    |> Keyword.merge(live_socket_options(config))
    |> override(:recbuf, options, :receive_buffer_bytes)
    |> override(:send_timeout, options, :send_timeout_ms)
    |> override(:sndbuf, options, :send_buffer_bytes)
  end

  # `buffer` is the user-space buffer that the driver allocates for each socket
  # that reads. It is always set, and set first: without it, `recbuf` also
  # raises this buffer. Without `recbuf` the kernel sizes the receive buffer.
  defp live_socket_options(config) do
    receive_buffer =
      case config.tcp_receive_buffer_bytes do
        :auto -> []
        bytes -> [recbuf: bytes]
      end

    [buffer: config.tcp_user_buffer_bytes] ++
      receive_buffer ++ [send_timeout: config.transport_send_timeout_ms]
  end

  defp override(socket_options, name, options, option) do
    case Keyword.fetch(options, option) do
      {:ok, value} -> Keyword.put(socket_options, name, value)
      :error -> socket_options
    end
  end

  defp listener_settings(config) do
    Map.take(config, [
      :connections_per_acceptor,
      :http_idle_timeout_ms,
      :tcp_user_buffer_bytes,
      :tcp_receive_buffer_bytes,
      :transport_send_timeout_ms
    ])
  end

  # The listen sockets, which give their options to each socket they accept,
  # and the sockets of the open connections.
  defp sockets(reference) do
    acceptors =
      for {:ranch_acceptors_sup, supervisor, _type, _modules} <-
            Supervisor.which_children(:ranch.info(reference).pid),
          do: supervisor

    for process <- acceptors ++ :ranch.procs(reference, :connections),
        {:links, links} <- [Process.info(process, :links)],
        socket <- links,
        is_port(socket),
        do: socket
  end

  defp protocol_options(options, config, reference) do
    handler_options =
      handler_options(
        config,
        Keyword.get(options, :budget_namespace, reference),
        Keyword.get(options, :max_websockets)
      )

    dispatch =
      :cowboy_router.compile([
        {:_,
         [
           {"/ws", PaseoRelay.Socket, handler_options},
           {:_, PaseoRelay.Operations, handler_options}
         ]}
      ])

    %{
      env: %{dispatch: dispatch},
      idle_timeout: Keyword.get(options, :http_idle_timeout_ms, config.().http_idle_timeout_ms),
      protocols: [:http]
    }
  end
end
