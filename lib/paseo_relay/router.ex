defmodule PaseoRelay.Router do
  use Plug.Router

  plug(:fetch_query_params)
  plug(:match)
  plug(:dispatch)

  get "/ws" do
    with {:ok, connection} <- PaseoRelay.Connection.from_query(conn.query_params),
         decision <- PaseoRelay.Ownership.route(connection.server_id, target()),
         {:local, owner, reservation} <- decision do
      conn
      |> WebSockAdapter.upgrade(
        PaseoRelay.Socket,
        %{connection: connection, owner: owner, reservation: reservation},
        compress: false,
        max_frame_size: 32 * 1024 * 1024
      )
      |> halt()
    else
      {:reroute, _target} = decision ->
        PaseoRelay.Metrics.inc(:reroute_responses)
        PaseoRelay.Reroute.response(conn, decision)

      {:unavailable, reason} ->
        send_resp(conn, 503, Atom.to_string(reason))

      {:error, message} ->
        send_resp(conn, 400, message)
    end
  end

  match _ do
    PaseoRelay.Operations.call(conn, [])
  end

  defp target, do: Application.fetch_env!(:paseo_relay, :ownership_target)
end
