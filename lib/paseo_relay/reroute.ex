defmodule PaseoRelay.Reroute do
  @moduledoc false

  def headers({:reroute, target}, header) when is_binary(target) and is_binary(header),
    do: %{header => target}

  def headers(_, _header), do: %{}

  def response(conn, decision) do
    header = Application.fetch_env!(:paseo_relay, :reroute_header)

    Enum.reduce(headers(decision, header), conn, fn {name, value}, conn ->
      Plug.Conn.put_resp_header(conn, name, value)
    end)
    |> Plug.Conn.send_resp(409, "")
  end
end
