defmodule PaseoRelay.SystemMemory do
  @moduledoc """
  Reads how much memory the host or the container can still give this node.

  The host values are `MemTotal` and `MemAvailable` in `/proc/meminfo`. A cgroup
  v2 memory limit bounds both: the total is at most `memory.max`, and the
  available memory is at most the room below `memory.max` plus the file cache
  that the kernel reclaims before it kills a process (`inactive_file` and
  `active_file` in `memory.stat`). On a system without `/proc/meminfo` the
  reading is `:unavailable`.
  """

  @paths [
    meminfo: "/proc/meminfo",
    cgroup_max: "/sys/fs/cgroup/memory.max",
    cgroup_current: "/sys/fs/cgroup/memory.current",
    cgroup_stat: "/sys/fs/cgroup/memory.stat"
  ]

  @type reading :: %{total_bytes: non_neg_integer(), available_bytes: non_neg_integer()}

  @doc """
  Reads the memory files. `:system_memory_paths` in the application environment
  replaces the default paths.
  """
  @spec read(keyword(Path.t())) :: {:ok, reading()} | :unavailable
  def read(paths \\ Application.get_env(:paseo_relay, :system_memory_paths, @paths)) do
    [meminfo, cgroup_max, cgroup_current, cgroup_stat] =
      for file <- [:meminfo, :cgroup_max, :cgroup_current, :cgroup_stat] do
        case File.read(Keyword.fetch!(paths, file)) do
          {:ok, contents} -> contents
          {:error, _reason} -> nil
        end
      end

    parse(meminfo, cgroup_max, cgroup_current, cgroup_stat)
  end

  @doc "Computes a reading from the contents of the files. `nil` is a missing file."
  @spec parse(String.t() | nil, String.t() | nil, String.t() | nil, String.t() | nil) ::
          {:ok, reading()} | :unavailable
  def parse(meminfo, cgroup_max, cgroup_current, cgroup_stat \\ nil) do
    with {:ok, total_kb} <- field(meminfo, ~r/^MemTotal:\s+(\d+) kB$/m),
         {:ok, available_kb} <- field(meminfo, ~r/^MemAvailable:\s+(\d+) kB$/m) do
      total = total_kb * 1024
      available = available_kb * 1024

      # `memory.max` holds "max" without a limit, so it has no number then.
      case {field(cgroup_max, ~r/^(\d+)$/m), field(cgroup_current, ~r/^(\d+)$/m)} do
        {{:ok, limit}, {:ok, current}} ->
          room = max(limit - current, 0) + reclaimable(cgroup_stat)
          {:ok, %{total_bytes: min(total, limit), available_bytes: min(available, room)}}

        _no_limit ->
          {:ok, %{total_bytes: total, available_bytes: available}}
      end
    end
  end

  defp reclaimable(cgroup_stat) do
    for name <- ["inactive_file", "active_file"],
        {:ok, bytes} <- [field(cgroup_stat, ~r/^#{name} (\d+)$/m)],
        reduce: 0 do
      sum -> sum + bytes
    end
  end

  defp field(nil, _pattern), do: :unavailable

  defp field(contents, pattern) do
    case Regex.run(pattern, contents) do
      [_line, number] -> {:ok, String.to_integer(number)}
      nil -> :unavailable
    end
  end
end
