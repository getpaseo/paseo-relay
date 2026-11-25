defmodule PaseoRelay.Operations do
  @moduledoc """
  Platform-neutral HTTP operations contract.

  `GET /health` reports liveness, `GET /ready` reports whether new relay work
  may be admitted, and `GET /metrics` exposes a small Prometheus-compatible
  surface. A drain is activated through `PaseoRelay.Drain.begin/0`; it never
  depends on a deployment provider's control plane.
  """
  @behaviour :cowboy_handler

  @impl true
  def init(request, options) do
    {status, content_type, body} = response(:cowboy_req.path(request), options)

    request =
      :cowboy_req.reply(
        status,
        %{"content-type" => content_type},
        body,
        request
      )

    {:ok, request, nil}
  end

  def response(path), do: response(path, operation_options(configured_runtime()))

  def response(path, %{minimum_cluster_size: _minimum_cluster_size} = config),
    do: response(path, operation_options(config))

  def response("/health", _options), do: {200, "application/json", ~s({"status":"ok"})}

  def response("/ready", %{config: config, connection_budget: budget}) do
    if ready_without_capacity?(config) and ready_capacity?(capacity_status(budget)) do
      {200, "application/json", ~s({"status":"ready"})}
    else
      {503, "application/json", ~s({"status":"unready"})}
    end
  end

  def response("/metrics", %{config: config, connection_budget: budget}) do
    capacity_status = capacity_status(budget)

    body =
      [
        "# HELP paseo_relay_ready Whether this node admits new relay work.",
        "# TYPE paseo_relay_ready gauge",
        "paseo_relay_ready #{if(ready?(config, capacity_status), do: 1, else: 0)}",
        "# HELP paseo_relay_draining Whether this node is draining.",
        "# TYPE paseo_relay_draining gauge",
        "paseo_relay_draining #{if(draining?(), do: 1, else: 0)}",
        PaseoRelay.Metrics.render(capacity_status)
      ]
      |> Enum.join("\n")
      |> Kernel.<>("\n")

    {200, "text/plain; version=0.0.4", body}
  end

  def response(_path, _config), do: {404, "text/plain", "not found\n"}

  @doc "Reports whether this node admits new relay work, as `GET /ready` does."
  def ready? do
    %{config: config, connection_budget: budget} = operation_options(configured_runtime())
    ready?(config, capacity_status(budget))
  end

  defp draining?, do: PaseoRelay.Drain.draining?()

  defp ready?(config, capacity_status) do
    ready_without_capacity?(config) and ready_capacity?(capacity_status)
  end

  defp ready_without_capacity?(config),
    do: not draining?() and PaseoRelay.Ownership.ready?(config.minimum_cluster_size)

  defp ready_capacity?({:available, %{admission: :open}}), do: true
  defp ready_capacity?(_unavailable_or_closed), do: false

  defp capacity_status({namespace, limit}), do: PaseoRelay.Capacity.status(namespace, limit)

  defp operation_options(config) do
    %{
      config: config,
      connection_budget: {
        PaseoRelay.Listener,
        config.acceptors * config.connections_per_acceptor
      }
    }
  end

  defp configured_runtime do
    :paseo_relay
    |> Application.get_env(:runtime, PaseoRelay.Config.defaults())
    |> PaseoRelay.Config.normalize()
  end
end
