import Config

config :paseo_relay,
  operations: [host: "127.0.0.1", ip: {127, 0, 0, 1}, port: 4000, drain: false],
  ownership_target: "local",
  reroute_header: "x-reroute-target",
  minimum_cluster_size: 1,
  cluster_query: nil
