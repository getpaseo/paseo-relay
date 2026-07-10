defmodule PaseoRelay.Operations do
  @moduledoc """
  Platform-neutral HTTP operations contract.

  `GET /health` reports liveness, `GET /ready` reports whether new relay work
  may be admitted, and `GET /metrics` exposes a small Prometheus-compatible
  surface. A drain is activated through `PaseoRelay.Drain.begin/0`; it never
  depends on a deployment provider's control plane.
  """
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/health" do
    send_json(conn, 200, "ok")
  end

  get "/ready" do
    if draining?(conn) do
      send_json(conn, 503, "draining")
    else
      send_json(conn, 200, "ready")
    end
  end

  get "/metrics" do
    body =
      [
        "# HELP paseo_relay_ready Whether this node admits new relay work.",
        "# TYPE paseo_relay_ready gauge",
        "paseo_relay_ready #{if(draining?(conn), do: 0, else: 1)}",
        "# HELP paseo_relay_draining Whether this node is draining.",
        "# TYPE paseo_relay_draining gauge",
        "paseo_relay_draining #{if(draining?(conn), do: 1, else: 0)}"
      ]
      |> Enum.join("\n")
      |> Kernel.<>("\n")

    conn
    |> put_resp_content_type("text/plain; version=0.0.4")
    |> send_resp(200, body)
  end

  match _ do
    send_resp(conn, 404, "not found\n")
  end

  defp draining?(conn) do
    _ = conn
    PaseoRelay.Drain.draining?()
  end

  defp send_json(conn, status, state) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, ~s({"status":"#{state}"}))
  end
end
