defmodule PaseoRelay.Application do
  use Application

  @impl true
  def start(_type, _args) do
    operations =
      Application.get_env(:paseo_relay, :operations,
        host: "127.0.0.1",
        ip: {127, 0, 0, 1},
        port: 4000,
        drain: false
      )

    children = [
      {DNSCluster, query: Application.get_env(:paseo_relay, :cluster_query) || :ignore},
      {PaseoRelay.Drain, Keyword.fetch!(operations, :drain)},
      PaseoRelay.Metrics,
      PaseoRelay.Registry,
      {Bandit,
       plug: PaseoRelay.Router,
       scheme: :http,
       ip: Keyword.fetch!(operations, :ip),
       port: Keyword.fetch!(operations, :port),
       websocket_options: [max_frame_size: 32 * 1024 * 1024, compress: false]}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PaseoRelay.Supervisor)
  end
end
