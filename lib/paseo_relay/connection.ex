defmodule PaseoRelay.Connection do
  @enforce_keys [:server_id, :role, :version, :connection_id]
  defstruct [:server_id, :role, :version, :connection_id]

  @type t :: %__MODULE__{
          server_id: String.t(),
          role: :server | :client,
          version: 1 | 2,
          connection_id: String.t() | nil
        }

  @spec from_query(map()) :: {:ok, t()} | {:error, String.t()}
  def from_query(query) do
    with {:ok, role} <- role(query["role"]),
         {:ok, server_id} <- server_id(query["serverId"]),
         {:ok, version} <- version(query["v"]) do
      {:ok,
       %__MODULE__{
         server_id: server_id,
         role: role,
         version: version,
         connection_id: connection_id(version, role, query["connectionId"])
       }}
    end
  end

  defp role("server"), do: {:ok, :server}
  defp role("client"), do: {:ok, :client}
  defp role(_), do: {:error, "Missing or invalid role parameter"}

  defp server_id(value) when is_binary(value) and byte_size(value) > 0, do: {:ok, value}
  defp server_id(_), do: {:error, "Missing serverId parameter"}

  defp version(nil), do: {:ok, 1}

  defp version(value) when is_binary(value) do
    case String.trim(value) do
      "" -> {:ok, 1}
      "1" -> {:ok, 1}
      "2" -> {:ok, 2}
      _ -> {:error, "Invalid v parameter (expected 1 or 2)"}
    end
  end

  defp connection_id(1, _role, _value), do: nil
  defp connection_id(2, :client, value) when value in [nil, ""], do: generated_connection_id()
  defp connection_id(2, _role, value) when is_binary(value), do: String.trim(value)
  defp connection_id(2, _role, _value), do: ""

  defp generated_connection_id do
    "conn_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end
end
