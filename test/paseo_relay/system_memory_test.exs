defmodule PaseoRelay.SystemMemoryTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.SystemMemory

  @gibibyte 1024 * 1024 * 1024

  @meminfo """
  MemTotal:       16384000 kB
  MemFree:          812340 kB
  MemAvailable:   11388000 kB
  Buffers:          204800 kB
  """

  test "an unlimited container reports the host memory" do
    assert SystemMemory.parse(@meminfo, "max\n", "1234\n") ==
             {:ok, %{total_bytes: 16_384_000 * 1024, available_bytes: 11_388_000 * 1024}}
  end

  test "a host without cgroup files reports the host memory" do
    assert SystemMemory.parse(@meminfo, nil, nil) ==
             {:ok, %{total_bytes: 16_384_000 * 1024, available_bytes: 11_388_000 * 1024}}
  end

  test "a container limit below the host memory bounds the total and the available memory" do
    assert SystemMemory.parse(@meminfo, "#{4 * @gibibyte}\n", "#{3 * @gibibyte}\n") ==
             {:ok, %{total_bytes: 4 * @gibibyte, available_bytes: @gibibyte}}
  end

  test "the host bounds a container whose limit has more room than the host" do
    assert SystemMemory.parse(@meminfo, "#{64 * @gibibyte}\n", "#{@gibibyte}\n") ==
             {:ok, %{total_bytes: 16_384_000 * 1024, available_bytes: 11_388_000 * 1024}}
  end

  test "a container above its limit has no available memory" do
    assert {:ok, %{available_bytes: 0}} =
             SystemMemory.parse(@meminfo, "#{@gibibyte}\n", "#{2 * @gibibyte}\n")
  end

  test "page cache that the container can reclaim counts as available" do
    stat = """
    anon 2147483648
    file 1610612736
    inactive_file 805306368
    active_file 268435456
    shmem 536870912
    """

    # 4 GiB limit, 3.5 GiB charged, of which 1 GiB is reclaimable file cache.
    assert SystemMemory.parse(@meminfo, "#{4 * @gibibyte}\n", "#{div(7 * @gibibyte, 2)}\n", stat) ==
             {:ok, %{total_bytes: 4 * @gibibyte, available_bytes: div(3 * @gibibyte, 2)}}
  end

  test "memory is unavailable without both host values" do
    assert SystemMemory.parse(nil, "max\n", "0\n") == :unavailable
    assert SystemMemory.parse("MemTotal: 16384000 kB\n", nil, nil) == :unavailable
    assert SystemMemory.parse("MemTotal: many kB\nMemAvailable: 1 kB\n", nil, nil) == :unavailable
  end

  test "reads the three files and tolerates missing cgroup files" do
    directory =
      Path.join(System.tmp_dir!(), "system_memory_#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)

    paths = [
      meminfo: Path.join(directory, "meminfo"),
      cgroup_max: Path.join(directory, "memory.max"),
      cgroup_current: Path.join(directory, "memory.current"),
      cgroup_stat: Path.join(directory, "memory.stat")
    ]

    assert SystemMemory.read(paths) == :unavailable

    File.write!(paths[:meminfo], @meminfo)

    assert SystemMemory.read(paths) ==
             {:ok, %{total_bytes: 16_384_000 * 1024, available_bytes: 11_388_000 * 1024}}

    File.write!(paths[:cgroup_max], "#{2 * @gibibyte}\n")
    File.write!(paths[:cgroup_current], "#{@gibibyte}\n")

    assert SystemMemory.read(paths) ==
             {:ok, %{total_bytes: 2 * @gibibyte, available_bytes: @gibibyte}}

    File.write!(paths[:cgroup_stat], "inactive_file #{div(@gibibyte, 2)}\nactive_file 0\n")

    assert SystemMemory.read(paths) ==
             {:ok, %{total_bytes: 2 * @gibibyte, available_bytes: div(3 * @gibibyte, 2)}}
  end
end
