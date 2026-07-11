defmodule PaseoRelay.Config do
  @moduledoc false

  @defaults %{
    host: "127.0.0.1",
    port: 4000,
    drain: false,
    acceptors: 100,
    connections_per_acceptor: 200,
    connection_retry_count: 5,
    connection_retry_wait_ms: 1_000,
    node_name: nil,
    cookie: nil
  }

  @spec load(Enumerable.t()) :: {:ok, map()} | {:error, String.t()}
  def load(environment \\ System.get_env()) do
    environment = Map.new(environment)
    host = Map.get(environment, "PASEO_RELAY_HOST", @defaults.host)

    with {:ok, ip} <- ip(host),
         {:ok, port} <- port(environment, "PASEO_RELAY_PORT", @defaults.port),
         {:ok, drain} <- boolean(environment, "PASEO_RELAY_DRAIN", @defaults.drain),
         {:ok, acceptors} <-
           integer(environment, "PASEO_RELAY_ACCEPTORS", @defaults.acceptors, 1..1_000),
         {:ok, connections_per_acceptor} <-
           integer(
             environment,
             "PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR",
             @defaults.connections_per_acceptor,
             1..1_000_000
           ),
         {:ok, connection_retry_count} <-
           integer(
             environment,
             "PASEO_RELAY_CONNECTION_RETRY_COUNT",
             @defaults.connection_retry_count,
             0..1_000
           ),
         {:ok, connection_retry_wait_ms} <-
           integer(
             environment,
             "PASEO_RELAY_CONNECTION_RETRY_WAIT_MS",
             @defaults.connection_retry_wait_ms,
             0..60_000
           ) do
      {:ok,
       %{
         host: host,
         ip: ip,
         port: port,
         drain: drain,
         acceptors: acceptors,
         connections_per_acceptor: connections_per_acceptor,
         connection_retry_count: connection_retry_count,
         connection_retry_wait_ms: connection_retry_wait_ms,
         node_name: Map.get(environment, "RELEASE_NODE"),
         cookie: Map.get(environment, "RELEASE_COOKIE")
       }}
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
end
