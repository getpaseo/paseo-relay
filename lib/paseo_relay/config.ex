defmodule PaseoRelay.Config do
  @moduledoc false

  @defaults %{
    host: "127.0.0.1",
    port: 4000,
    internal_port: 4001,
    drain: false,
    node_name: nil,
    cookie: nil
  }

  @spec load(Enumerable.t()) :: {:ok, map()} | {:error, String.t()}
  def load(environment \\ System.get_env()) do
    environment = Map.new(environment)
    host = Map.get(environment, "PASEO_RELAY_HOST", @defaults.host)

    with {:ok, ip} <- ip(host),
         {:ok, port} <- port(environment, "PASEO_RELAY_PORT", @defaults.port),
         {:ok, internal_port} <-
           port(environment, "PASEO_RELAY_INTERNAL_PORT", @defaults.internal_port),
         {:ok, drain} <- boolean(environment, "PASEO_RELAY_DRAIN", @defaults.drain) do
      {:ok,
       %{
         host: host,
         ip: ip,
         port: port,
         internal_port: internal_port,
         drain: drain,
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

  defp boolean(environment, key, default) do
    case Map.get(environment, key) do
      nil -> {:ok, default}
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, "#{key} must be true or false"}
    end
  end
end
