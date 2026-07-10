defmodule PaseoRelay.Application do
  use Application

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:paseo_relay, :port, 4000)

    children = [
      PaseoRelay.Registry,
      {Bandit,
       plug: PaseoRelay.Router,
       port: port,
       websocket_options: [max_frame_size: 32 * 1024 * 1024, compress: false]}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PaseoRelay.Supervisor)
  end
end
