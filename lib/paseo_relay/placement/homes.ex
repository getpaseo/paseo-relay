defmodule PaseoRelay.Placement.Homes do
  @moduledoc false

  # Bounded last-write-wins map of server_id -> home node.
  #
  # Every node holds a replica. A write wins when its `updated_at_ms` (system
  # time, so it compares across nodes) is newer; equal times break on the node
  # name so every replica picks the same entry. An ordered age index keeps
  # eviction O(log n). An entry not written for `max_idle_ms` is removed unless
  # its session is still active, and a new entry past `max_entries` evicts the
  # oldest. Only the owning process writes; any process reads.

  @enforce_keys [:table, :ages, :max_entries, :max_idle_ms]
  defstruct [:table, :ages, :max_entries, :max_idle_ms]

  @default_max_entries 200_000
  @default_max_idle_ms 7 * 24 * 60 * 60 * 1_000

  def new(name, options \\ []) do
    homes = %__MODULE__{
      table: name,
      ages: :"#{name}_ages",
      max_entries: Keyword.get(options, :max_entries, @default_max_entries),
      max_idle_ms: Keyword.get(options, :max_idle_ms, @default_max_idle_ms)
    }

    :ets.new(homes.table, [:named_table, :protected, :set, read_concurrency: true])
    :ets.new(homes.ages, [:named_table, :protected, :ordered_set])
    homes
  end

  @doc "Changes the idle limit that `evict_idle/4` applies."
  def configure(%__MODULE__{} = homes, max_idle_ms), do: %{homes | max_idle_ms: max_idle_ms}

  @doc "Reads the home of `server_id` from the table `table`, from any process."
  def home(table, server_id) when is_atom(table) do
    case :ets.lookup(table, server_id) do
      [{^server_id, home, _updated_at_ms}] -> home
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "Writes the entry when it is newer. Returns `:updated` or `:stale`."
  def put(%__MODULE__{} = homes, server_id, home, updated_at_ms) do
    case :ets.lookup(homes.table, server_id) do
      [{_server_id, current, current_at}] when {updated_at_ms, home} <= {current_at, current} ->
        :stale

      [{_server_id, _current, current_at}] ->
        :ets.delete(homes.ages, {current_at, server_id})
        write(homes, server_id, home, updated_at_ms)

      [] ->
        write(homes, server_id, home, updated_at_ms)
        evict_over_capacity(homes)
        :updated
    end
  end

  def entries(%__MODULE__{table: table}), do: :ets.tab2list(table)

  def size(%__MODULE__{table: table}), do: :ets.info(table, :size)

  @doc """
  Visits up to `limit` entries that were not written for `max_idle_ms`. An
  entry for which `active?.(server_id, home)` is true is rewritten at `now_ms`;
  any other is removed.
  """
  def evict_idle(%__MODULE__{} = homes, now_ms, limit, active?) do
    evict_before(homes, now_ms - homes.max_idle_ms, now_ms, limit, active?)
  end

  defp write(homes, server_id, home, updated_at_ms) do
    :ets.insert(homes.table, {server_id, home, updated_at_ms})
    :ets.insert(homes.ages, {{updated_at_ms, server_id}})
    :updated
  end

  defp evict_over_capacity(homes) do
    if size(homes) > homes.max_entries, do: evict_oldest(homes)
  end

  defp evict_oldest(homes) do
    case :ets.first(homes.ages) do
      :"$end_of_table" -> :ok
      {_updated_at_ms, server_id} = key -> remove(homes, key, server_id)
    end
  end

  defp evict_before(_homes, _cutoff_ms, _now_ms, 0, _active?), do: :ok

  defp evict_before(homes, cutoff_ms, now_ms, limit, active?) do
    case :ets.first(homes.ages) do
      {updated_at_ms, server_id} = key when updated_at_ms < cutoff_ms ->
        [{^server_id, home, ^updated_at_ms}] = :ets.lookup(homes.table, server_id)
        remove(homes, key, server_id)
        if active?.(server_id, home), do: write(homes, server_id, home, now_ms)
        evict_before(homes, cutoff_ms, now_ms, limit - 1, active?)

      _newer_or_empty ->
        :ok
    end
  end

  defp remove(homes, key, server_id) do
    :ets.delete(homes.ages, key)
    :ets.delete(homes.table, server_id)
  end
end
