defmodule PaseoRelay.Application do
  use Application

  @impl true
  def start(_type, _args) do
    config =
      PaseoRelay.Config.normalize(
        Application.get_env(:paseo_relay, :runtime, PaseoRelay.Config.defaults())
      )

    children = [
      {DNSCluster, query: config.cluster_query || :ignore},
      {PaseoRelay.StaticCluster, nodes: config.cluster_nodes},
      {PaseoRelay.Drain, config.drain},
      PaseoRelay.Metrics,
      {PaseoRelay.RuntimeSupervisor, config}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PaseoRelay.Supervisor)
  end
end
