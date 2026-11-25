defmodule PaseoRelay.Metrics do
  @moduledoc false

  use GenServer

  @metrics [
    {:active_websockets, :gauge, "active_websockets", "Open WebSocket connections on this node."},
    {:active_sessions, :gauge, "active_sessions", "Relay sessions owned by this node."},
    {:reroute_responses, :counter, "reroute_responses_total",
     "WebSocket upgrades rerouted to another owner."},
    {:connection_rejections, :counter, "connection_rejections_total",
     "WebSocket upgrades rejected at configured capacity or during memory pressure."},
    {:frames_forwarded, :counter, "frames_forwarded_total",
     "WebSocket frames forwarded by this node."},
    {:bytes_forwarded, :counter, "bytes_forwarded_total",
     "WebSocket payload bytes forwarded by this node."},
    {:ingress_reserved_bytes, :gauge, "ingress_reserved_bytes",
     "Weighted ingress bytes admitted on this node."},
    {:inflight_delivery_bytes, :gauge, "inflight_delivery_bytes",
     "Payload bytes currently held by synchronous downstream delivery."},
    {:backpressured_sources, :gauge, "backpressured_sources",
     "Source WebSockets currently waiting for downstream delivery."},
    {:slow_consumer_disconnects, :counter, "slow_consumer_disconnects_total",
     "Destinations disconnected after exceeding a delivery deadline."},
    {:delivery_timeouts, :counter, "delivery_timeouts_total",
     "Synchronous downstream deliveries that exceeded their deadline."},
    {:memory_pressure_disconnects, :counter, "memory_pressure_disconnects_total",
     "WebSockets closed by node memory-pressure recovery."},
    {:max_frame_bytes, :gauge, "max_frame_bytes",
     "Largest WebSocket frame payload observed since node start."},
    {:memory_watermark_bytes, :gauge, "memory_watermark_bytes",
     "BEAM memory watermark that Capacity applies now; 0 means that the OOM guard decides."},
    {:system_memory_total_bytes, :gauge, "system_memory_total_bytes",
     "Memory of the host, or the container limit when it is lower; 0 when unreadable."},
    {:system_memory_available_bytes, :gauge, "system_memory_available_bytes",
     "Memory that the host or the container can still give; 0 when unreadable."},
    {:oom_floor_bytes, :gauge, "oom_floor_bytes",
     "Available memory below which the OOM guard sheds WebSockets; 0 means disabled."},
    {:misplaced_sessions, :gauge, "misplaced_sessions",
     "Sessions owned by this node whose home is another node."},
    {:sessions_healed, :counter, "sessions_healed_total",
     "Session Owners this node stopped so that the daemon reconnects to its home node."},
    {:home_promotions, :counter, "home_promotions_total",
     "Session homes moved to this node after a daemon returned here twice after a heal."},
    {:beam_total_memory, :gauge, "beam_total_memory_bytes", "Total memory allocated by BEAM."},
    {:beam_process_memory, :gauge, "beam_process_memory_bytes",
     "Memory allocated by BEAM processes."},
    {:beam_binary_memory, :gauge, "beam_binary_memory_bytes",
     "Memory allocated for BEAM binaries."},
    {:beam_ets_memory, :gauge, "beam_ets_memory_bytes", "Memory allocated for BEAM ETS tables."}
  ]

  @delivery_buckets [
    {1_000, :delivery_wait_le_1ms, "0.001"},
    {10_000, :delivery_wait_le_10ms, "0.01"},
    {100_000, :delivery_wait_le_100ms, "0.1"},
    {1_000_000, :delivery_wait_le_1s, "1"},
    {10_000_000, :delivery_wait_le_10s, "10"}
  ]
  @maximum_message_payload_bytes PaseoRelay.Protocol.maximum_message_payload_bytes()
  @frame_buckets [
    {1024, :frame_size_le_1k, "1024"},
    {64 * 1024, :frame_size_le_64k, "65536"},
    {1024 * 1024, :frame_size_le_1m, "1048576"},
    {8 * 1024 * 1024, :frame_size_le_8m, "8388608"},
    {@maximum_message_payload_bytes, :frame_size_le_32m,
     Integer.to_string(@maximum_message_payload_bytes)}
  ]
  @capacity_names ~w(active_websockets ingress_reserved_bytes inflight_delivery_bytes backpressured_sources memory_watermark_bytes system_memory_total_bytes system_memory_available_bytes oom_floor_bytes)a
  @computed_names @capacity_names ++
                    ~w(active_sessions misplaced_sessions max_frame_bytes beam_total_memory beam_process_memory beam_binary_memory beam_ets_memory)a
  @handshake_outcomes [:accepted, :rejected]
  @handshake_versions [1, 2]
  @handshake_types [:hello, :e2ee_hello]
  @handshake_counter_names for outcome <- @handshake_outcomes,
                               version <- @handshake_versions,
                               type <- @handshake_types,
                               do: {:handshake, outcome, version, type}
  @counter_names (@metrics |> Enum.map(&elem(&1, 0)) |> Kernel.--(@computed_names)) ++
                   @handshake_counter_names ++
                   [
                     :delivery_wait_microseconds,
                     :delivery_wait_count,
                     :frame_size_count,
                     :frame_size_bytes_sum
                   ] ++
                   Enum.map(@delivery_buckets, &elem(&1, 1)) ++
                   Enum.map(@frame_buckets, &elem(&1, 1))

  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def inc(name, amount \\ 1), do: :counters.add(counters(), index(name), amount)
  def dec(name, amount \\ 1), do: inc(name, -amount)

  def value(name) when name in @capacity_names, do: PaseoRelay.Capacity.value(name)

  def value(:misplaced_sessions), do: PaseoRelay.Placement.misplaced_sessions()
  def value(:active_sessions), do: :syn.local_registry_count(:paseo_relay_owners)
  def value(:beam_total_memory), do: :erlang.memory(:total)
  def value(:beam_process_memory), do: :erlang.memory(:processes)
  def value(:beam_binary_memory), do: :erlang.memory(:binary)
  def value(:beam_ets_memory), do: :erlang.memory(:ets)
  def value(:max_frame_bytes), do: :atomics.get(max_frame(), 1)
  def value(name), do: :counters.get(counters(), index(name))

  def snapshot, do: snapshot(PaseoRelay.Capacity.snapshot())

  def snapshot(capacity) when is_map(capacity) do
    @metrics
    |> Enum.map(&elem(&1, 0))
    |> Enum.reduce(%{}, fn name, values ->
      if name in @capacity_names and not Map.has_key?(capacity, name) do
        values
      else
        Map.put(values, name, snapshot_value(name, capacity))
      end
    end)
  end

  def observe_delivery_wait(native_duration) do
    inc(:delivery_wait_count)

    inc(
      :delivery_wait_microseconds,
      microseconds = System.convert_time_unit(native_duration, :native, :microsecond)
    )

    Enum.each(@delivery_buckets, fn {limit, name, _label} ->
      if microseconds <= limit, do: inc(name)
    end)
  end

  def observe_frame(byte_count) do
    inc(:frame_size_count)
    inc(:frame_size_bytes_sum, byte_count)

    Enum.each(@frame_buckets, fn {limit, name, _label} ->
      if byte_count <= limit, do: inc(name)
    end)

    put_max_frame(byte_count)
  end

  def observe_handshake(outcome, version, type)
      when outcome in @handshake_outcomes and version in @handshake_versions and
             type in @handshake_types do
    inc({:handshake, outcome, version, type})
  end

  def render(capacity_status) do
    capacity =
      case capacity_status do
        {:available, %{gauges: gauges}} -> gauges
        :unavailable -> %{}
      end

    [
      render_metrics(snapshot(capacity)),
      render_handshake_metrics(),
      render_delivery_histogram(),
      render_frame_histogram()
    ]
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  @impl true
  def init(:ok) do
    _ = counters()
    _ = max_frame()
    {:ok, :metrics}
  end

  defp counters do
    case :persistent_term.get(__MODULE__, nil) do
      nil ->
        counters = :counters.new(length(@counter_names), [:write_concurrency])
        :persistent_term.put(__MODULE__, counters)
        counters

      counters ->
        counters
    end
  end

  defp max_frame do
    key = {__MODULE__, :max_frame}

    case :persistent_term.get(key, nil) do
      nil ->
        atomics = :atomics.new(1, [])
        :persistent_term.put(key, atomics)
        atomics

      atomics ->
        atomics
    end
  end

  defp put_max_frame(byte_count) do
    atomics = max_frame()
    current = :atomics.get(atomics, 1)

    cond do
      byte_count <= current -> :ok
      :atomics.compare_exchange(atomics, 1, current, byte_count) == :ok -> :ok
      true -> put_max_frame(byte_count)
    end
  end

  defp index(name), do: Enum.find_index(@counter_names, &(&1 == name)) + 1

  defp render_metrics(values) do
    @metrics
    |> Enum.filter(fn {name, _type, _public_name, _help} -> Map.has_key?(values, name) end)
    |> Enum.map_join("\n", fn {name, type, public_name, help} ->
      full_name = "paseo_relay_#{public_name}"

      [
        "# HELP #{full_name} #{help}",
        "# TYPE #{full_name} #{type}",
        "#{full_name} #{Map.fetch!(values, name)}"
      ]
      |> Enum.join("\n")
    end)
  end

  defp snapshot_value(name, capacity) when name in @capacity_names,
    do: Map.fetch!(capacity, name)

  defp snapshot_value(name, _capacity), do: value(name)

  defp render_delivery_histogram do
    name = "paseo_relay_delivery_wait_seconds"

    buckets =
      Enum.map(@delivery_buckets, fn {_limit, counter, label} ->
        ~s(#{name}_bucket{le="#{label}"} #{value(counter)})
      end)

    ([
       "# HELP #{name} Time a source waits for synchronous downstream delivery.",
       "# TYPE #{name} histogram"
     ] ++
       buckets ++
       [
         ~s(#{name}_bucket{le="+Inf"} #{value(:delivery_wait_count)}),
         "#{name}_sum #{value(:delivery_wait_microseconds) / 1_000_000}",
         "#{name}_count #{value(:delivery_wait_count)}"
       ])
    |> Enum.join("\n")
  end

  defp render_handshake_metrics do
    Enum.map_join(@handshake_outcomes, "\n", fn outcome ->
      name = "paseo_relay_handshake_#{outcome}_total"

      series =
        for version <- @handshake_versions, type <- @handshake_types do
          ~s(#{name}{routing_version="v#{version}",type="#{type}"} #{value({:handshake, outcome, version, type})})
        end

      ([
         "# HELP #{name} Client E2EE handshake frames #{outcome} by the handshake input validator.",
         "# TYPE #{name} counter"
       ] ++ series)
      |> Enum.join("\n")
    end)
  end

  defp render_frame_histogram do
    name = "paseo_relay_frame_size_bytes"

    buckets =
      Enum.map(@frame_buckets, fn {_limit, counter, label} ->
        ~s(#{name}_bucket{le="#{label}"} #{value(counter)})
      end)

    (["# HELP #{name} WebSocket payload-size distribution.", "# TYPE #{name} histogram"] ++
       buckets ++
       [
         ~s(#{name}_bucket{le="+Inf"} #{value(:frame_size_count)}),
         "#{name}_sum #{value(:frame_size_bytes_sum)}",
         "#{name}_count #{value(:frame_size_count)}"
       ])
    |> Enum.join("\n")
  end
end
