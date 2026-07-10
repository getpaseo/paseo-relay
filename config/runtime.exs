import Config

if config_env() == :prod do
  {:ok, operations} = PaseoRelay.Config.load()

  config :paseo_relay,
    operations: Map.to_list(operations),
    ownership_target: System.get_env("PASEO_RELAY_OWNERSHIP_TARGET", "local"),
    reroute_header: System.get_env("PASEO_RELAY_REROUTE_HEADER", "x-reroute-target"),
    minimum_cluster_size: String.to_integer(System.get_env("PASEO_RELAY_MIN_CLUSTER_SIZE", "1")),
    cluster_query: System.get_env("PASEO_RELAY_CLUSTER_QUERY")
end
