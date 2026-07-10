defmodule PaseoRelay.Router do
  use Plug.Router

  plug(:fetch_query_params)
  plug(:match)
  plug(:dispatch)

  get "/ws" do
    with {:ok, connection} <- PaseoRelay.Connection.from_query(conn.query_params) do
      conn
      |> WebSockAdapter.upgrade(PaseoRelay.Socket, connection,
        compress: false,
        max_frame_size: 32 * 1024 * 1024
      )
      |> halt()
    else
      {:error, message} -> send_resp(conn, 400, message)
    end
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end
