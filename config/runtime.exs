import Config

if config_env() == :prod do
  {:ok, operations} = PaseoRelay.Config.load()
  config :paseo_relay, operations: Map.to_list(operations)
end
