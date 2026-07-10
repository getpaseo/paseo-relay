defmodule PaseoRelay.MixProject do
  use Mix.Project

  def project do
    [
      app: :paseo_relay,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      releases: [paseo_relay: [include_executables_for: [:unix]]],
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {PaseoRelay.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:bandit, "~> 1.8"},
      {:jason, "~> 1.4"},
      {:plug, "~> 1.18"},
      {:websock_adapter, "~> 0.5"},
      {:websockex, "~> 0.4", only: :test}
    ]
  end
end
