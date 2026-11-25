defmodule PaseoRelay.Application do
  use Application

  @impl true
  def start(_type, _args) do
    config = PaseoRelay.RuntimeConfig.boot()

    children = [
      {DNSCluster, query: config.cluster_query || :ignore},
      PaseoRelay.StaticCluster,
      PaseoRelay.Metrics,
      PaseoRelay.Placement,
      PaseoRelay.RuntimeSupervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PaseoRelay.Supervisor)
  end
end
