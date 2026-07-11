defmodule PaseoRelay.Application do
  use Application

  @impl true
  def start(_type, _args) do
    operations =
      Application.get_env(:paseo_relay, :operations,
        host: "127.0.0.1",
        ip: {127, 0, 0, 1},
        port: 4000,
        drain: false,
        acceptors: 100,
        connections_per_acceptor: 200,
        connection_retry_count: 5,
        connection_retry_wait_ms: 1_000
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
       thousand_island_options: listener_options(operations),
       websocket_options: [max_frame_size: 32 * 1024 * 1024, compress: false]}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PaseoRelay.Supervisor)
  end

  defp listener_options(operations) do
    [
      num_acceptors: Keyword.get(operations, :acceptors, 100),
      num_connections: Keyword.get(operations, :connections_per_acceptor, 200),
      max_connections_retry_count: Keyword.get(operations, :connection_retry_count, 5),
      max_connections_retry_wait: Keyword.get(operations, :connection_retry_wait_ms, 1_000)
    ]
  end
end
