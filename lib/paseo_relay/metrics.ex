defmodule PaseoRelay.Metrics do
  @moduledoc false

  use GenServer

  @names [
    :active_websockets,
    :active_sessions,
    :reroute_responses,
    :frames_forwarded,
    :bytes_forwarded
  ]

  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def inc(name, amount \\ 1), do: :counters.add(counters(), index(name), amount)
  def dec(name, amount \\ 1), do: inc(name, -amount)
  def value(name), do: :counters.get(counters(), index(name))

  def snapshot do
    Map.new(@names, &{&1, value(&1)})
  end

  def render do
    snapshot()
    |> Enum.map_join("\n", fn {name, value} -> "paseo_relay_#{name} #{value}" end)
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

  defp counters, do: :persistent_term.get(__MODULE__)
  defp index(name), do: Enum.find_index(@names, &(&1 == name)) + 1
end
