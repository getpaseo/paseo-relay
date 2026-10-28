defmodule PaseoRelay.Config do
  @moduledoc false

  defstruct host: "127.0.0.1",
            ip: {127, 0, 0, 1},
            port: 4000,
            drain: false,
            acceptors: 100,
            connections_per_acceptor: 200,
            http_idle_timeout_ms: 15_000,
            capacity_mutation_timeout_ms: 5_000,
            ingress_budget_bytes: 512 * 1024 * 1024,
            ingress_weight: 4,
            delivery_timeout_ms: 30_000,
            transport_send_timeout_ms: 35_000,
            control_queue_bytes: 1024 * 1024,
            data_attach_timeout_ms: 15_000,
            tcp_receive_buffer_bytes: 64 * 1024,
            websocket_max_heap_words: 32 * 1024 * 1024,
            memory_watermark_bytes: 0,
            ownership_target: "local",
            reroute_header: "x-reroute-target",
            minimum_cluster_size: 1,
            cluster_query: nil,
            cluster_nodes: [],
            node_name: nil,
            cookie: nil

  @type t :: %__MODULE__{}

  @spec load(Enumerable.t()) :: {:ok, t()} | {:error, String.t()}
  def load(environment \\ System.get_env()) do
    environment = Map.new(environment)
    defaults = defaults()
    host = Map.get(environment, "PASEO_RELAY_HOST", defaults.host)

    with {:ok, cluster_nodes} <- cluster_nodes(environment),
         {:ok, ip} <- ip(host),
         {:ok, port} <- port(environment, "PASEO_RELAY_PORT", defaults.port),
         {:ok, drain} <- boolean(environment, "PASEO_RELAY_DRAIN", defaults.drain),
         {:ok, acceptors} <-
           integer(environment, "PASEO_RELAY_ACCEPTORS", defaults.acceptors, 1..1_000),
         {:ok, connections_per_acceptor} <-
           integer(
             environment,
             "PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR",
             defaults.connections_per_acceptor,
             1..1_000_000
           ),
         {:ok, http_idle_timeout_ms} <-
           integer(
             environment,
             "PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS",
             defaults.http_idle_timeout_ms,
             100..120_000
           ),
         {:ok, capacity_mutation_timeout_ms} <-
           integer(
             environment,
             "PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS",
             defaults.capacity_mutation_timeout_ms,
             100..120_000
           ),
         {:ok, ingress_budget_bytes} <-
           integer(
             environment,
             "PASEO_RELAY_INGRESS_BUDGET_BYTES",
             defaults.ingress_budget_bytes,
             (128 * 1024 * 1024)..(8 * 1024 * 1024 * 1024)
           ),
         {:ok, ingress_weight} <-
           integer(environment, "PASEO_RELAY_INGRESS_WEIGHT", defaults.ingress_weight, 1..16),
         :ok <- validate_ingress_budget(ingress_budget_bytes, ingress_weight),
         {:ok, delivery_timeout_ms} <-
           integer(
             environment,
             "PASEO_RELAY_DELIVERY_TIMEOUT_MS",
             defaults.delivery_timeout_ms,
             100..120_000
           ),
         {:ok, transport_send_timeout_ms} <-
           integer(
             environment,
             "PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS",
             defaults.transport_send_timeout_ms,
             100..300_000
           ),
         :ok <- validate_delivery_timeouts(delivery_timeout_ms, transport_send_timeout_ms),
         {:ok, control_queue_bytes} <-
           integer(
             environment,
             "PASEO_RELAY_CONTROL_QUEUE_BYTES",
             defaults.control_queue_bytes,
             64..(64 * 1024 * 1024)
           ),
         {:ok, data_attach_timeout_ms} <-
           integer(
             environment,
             "PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS",
             defaults.data_attach_timeout_ms,
             1_000..120_000
           ),
         {:ok, tcp_receive_buffer_bytes} <-
           integer(
             environment,
             "PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES",
             defaults.tcp_receive_buffer_bytes,
             (4 * 1024)..(1024 * 1024)
           ),
         {:ok, websocket_max_heap_words} <-
           integer(
             environment,
             "PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS",
             defaults.websocket_max_heap_words,
             (32 * 1024 * 1024)..(128 * 1024 * 1024)
           ),
         {:ok, memory_watermark_bytes} <-
           disabled_or_integer(
             environment,
             "PASEO_RELAY_MEMORY_WATERMARK_BYTES",
             defaults.memory_watermark_bytes,
             (256 * 1024 * 1024)..(64 * 1024 * 1024 * 1024)
           ),
         {:ok, minimum_cluster_size} <-
           integer(
             environment,
             "PASEO_RELAY_MIN_CLUSTER_SIZE",
             defaults.minimum_cluster_size,
             1..1_000
           ) do
      {:ok,
       struct!(__MODULE__, %{
         host: host,
         ip: ip,
         port: port,
         drain: drain,
         acceptors: acceptors,
         connections_per_acceptor: connections_per_acceptor,
         http_idle_timeout_ms: http_idle_timeout_ms,
         capacity_mutation_timeout_ms: capacity_mutation_timeout_ms,
         ingress_budget_bytes: ingress_budget_bytes,
         ingress_weight: ingress_weight,
         delivery_timeout_ms: delivery_timeout_ms,
         transport_send_timeout_ms: transport_send_timeout_ms,
         control_queue_bytes: control_queue_bytes,
         data_attach_timeout_ms: data_attach_timeout_ms,
         tcp_receive_buffer_bytes: tcp_receive_buffer_bytes,
         websocket_max_heap_words: websocket_max_heap_words,
         memory_watermark_bytes: memory_watermark_bytes,
         ownership_target:
           Map.get(environment, "PASEO_RELAY_OWNERSHIP_TARGET", defaults.ownership_target),
         reroute_header:
           Map.get(environment, "PASEO_RELAY_REROUTE_HEADER", defaults.reroute_header),
         minimum_cluster_size: minimum_cluster_size,
         cluster_query: Map.get(environment, "PASEO_RELAY_CLUSTER_QUERY"),
         cluster_nodes: cluster_nodes,
         node_name: Map.get(environment, "RELEASE_NODE"),
         cookie: Map.get(environment, "RELEASE_COOKIE")
       })}
    end
  end

  @spec defaults() :: t()
  def defaults, do: %__MODULE__{}

  @spec normalize(t() | map() | keyword()) :: t()
  def normalize(%__MODULE__{} = config), do: config
  def normalize(settings), do: struct!(__MODULE__, Map.new(settings))

  defp cluster_nodes(environment) do
    nodes =
      environment
      |> Map.get("PASEO_RELAY_CLUSTER_NODES", "")
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.uniq()

    if length(nodes) <= 1_000 and
         Enum.all?(nodes, &(byte_size(&1) <= 255 and Regex.match?(~r/^[^\s@,]+@[^\s@,]+$/, &1))) do
      {:ok, Enum.map(nodes, &String.to_atom/1)}
    else
      {:error,
       "PASEO_RELAY_CLUSTER_NODES must contain at most 1000 comma-separated name@host peers"}
    end
  end

  defp ip(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> {:ok, address}
      {:error, :einval} -> {:error, "PASEO_RELAY_HOST must be an IP address"}
    end
  end

  defp port(environment, key, default) do
    case Map.get(environment, key) do
      nil ->
        {:ok, default}

      value ->
        case Integer.parse(value) do
          {port, ""} when port in 1..65_535 -> {:ok, port}
          _ -> {:error, "#{key} must be an integer between 1 and 65535"}
        end
    end
  end

  defp integer(environment, key, default, range) do
    case Map.get(environment, key) do
      nil ->
        {:ok, default}

      value ->
        case Integer.parse(value) do
          {integer, ""} ->
            if integer in range do
              {:ok, integer}
            else
              {:error, "#{key} must be an integer between #{range.first} and #{range.last}"}
            end

          _ ->
            {:error, "#{key} must be an integer between #{range.first} and #{range.last}"}
        end
    end
  end

  defp boolean(environment, key, default) do
    case Map.get(environment, key) do
      nil -> {:ok, default}
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, "#{key} must be true or false"}
    end
  end

  defp disabled_or_integer(environment, key, default, range) do
    case Map.get(environment, key) do
      nil -> {:ok, default}
      "0" -> {:ok, 0}
      _value -> integer(environment, key, default, range)
    end
  end

  defp validate_ingress_budget(budget, weight) do
    if budget >= PaseoRelay.Protocol.maximum_message_payload_bytes() * weight do
      :ok
    else
      {:error,
       "PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message at the configured weight"}
    end
  end

  defp validate_delivery_timeouts(delivery_timeout_ms, transport_send_timeout_ms) do
    if delivery_timeout_ms < transport_send_timeout_ms do
      :ok
    else
      {:error,
       "PASEO_RELAY_DELIVERY_TIMEOUT_MS must be lower than PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS"}
    end
  end
end
