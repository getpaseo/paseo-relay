defmodule PaseoRelay.Metrics do
  @moduledoc false

  use GenServer

  @metrics [
    {:active_websockets, :gauge, "active_websockets", "Open WebSocket connections on this node."},
    {:active_sessions, :gauge, "active_sessions", "Relay sessions owned by this node."},
    {:reroute_responses, :counter, "reroute_responses_total",
     "WebSocket upgrades rerouted to another owner."},
    {:frames_forwarded, :counter, "frames_forwarded_total",
     "WebSocket frames forwarded by this node."},
    {:bytes_forwarded, :counter, "bytes_forwarded_total",
     "WebSocket payload bytes forwarded by this node."}
  ]

  @names Enum.map(@metrics, &elem(&1, 0))

  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def inc(name, amount \\ 1), do: :counters.add(counters(), index(name), amount)
  def dec(name, amount \\ 1), do: inc(name, -amount)
  def value(name), do: :counters.get(counters(), index(name))

  def snapshot do
    Map.new(@names, &{&1, value(&1)})
  end

  def render do
    @metrics
    |> Enum.map_join("\n", fn {name, type, public_name, help} ->
      full_name = "paseo_relay_#{public_name}"

      [
        "# HELP #{full_name} #{help}",
        "# TYPE #{full_name} #{type}",
        "#{full_name} #{value(name)}"
      ]
      |> Enum.join("\n")
    end)
    |> Kernel.<>("\n")
  end

  @impl true
  def init(:ok) do
    :persistent_term.put(
      __MODULE__,
      :counters.new(length(@names), [:write_concurrency])
    )

    {:ok, :metrics}
  end

  defp counters do
    case :persistent_term.get(__MODULE__, nil) do
      nil ->
        counters = :counters.new(length(@names), [:write_concurrency])
        :persistent_term.put(__MODULE__, counters)
        counters

      counters ->
        counters
    end
  end

  defp index(name), do: Enum.find_index(@names, &(&1 == name)) + 1
end
