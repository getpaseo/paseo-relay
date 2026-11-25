defmodule PaseoRelay.PlacementHomesTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.Placement.Homes

  setup do
    %{name: :"homes_test_#{System.unique_integer([:positive])}"}
  end

  test "the newest write wins in every replica", %{name: name} do
    homes = Homes.new(name)

    assert Homes.put(homes, "s", :a@host, 100) == :updated
    assert Homes.put(homes, "s", :b@host, 99) == :stale
    assert Homes.home(name, "s") == :a@host
    assert Homes.put(homes, "s", :b@host, 101) == :updated
    assert Homes.home(name, "s") == :b@host
    # Equal times resolve the same way whatever the arrival order.
    assert Homes.put(homes, "s", :a@host, 101) == :stale
    assert Homes.put(homes, "s", :c@host, 101) == :updated
    assert Homes.home(name, "s") == :c@host
  end

  test "a full map evicts its least recently written entry", %{name: name} do
    homes = Homes.new(name, max_entries: 3)

    Homes.put(homes, "old", :a@host, 1)
    Homes.put(homes, "refreshed", :a@host, 2)
    Homes.put(homes, "middle", :a@host, 3)
    Homes.put(homes, "refreshed", :a@host, 4)
    Homes.put(homes, "new", :a@host, 5)

    assert Homes.size(homes) == 3
    assert Homes.home(name, "old") == nil
    assert Enum.all?(["refreshed", "middle", "new"], &(Homes.home(name, &1) == :a@host))
  end

  test "entries idle for the maximum age are evicted in bounded batches", %{name: name} do
    homes = Homes.new(name, max_idle_ms: 1_000)

    for index <- 1..5, do: Homes.put(homes, "idle_#{index}", :a@host, index)
    Homes.put(homes, "fresh", :a@host, 2_000)

    Homes.evict_idle(homes, 2_500, 3, &inactive/2)
    assert Homes.size(homes) == 3
    Homes.evict_idle(homes, 2_500, 3, &inactive/2)
    assert Homes.size(homes) == 1
    assert Homes.home(name, "fresh") == :a@host
  end

  test "an idle entry whose session is active is rewritten instead of evicted", %{name: name} do
    homes = Homes.new(name, max_idle_ms: 1_000)

    for index <- 1..4, do: Homes.put(homes, "active_#{index}", :a@host, index)
    Homes.put(homes, "ended", :a@host, 5)
    active? = fn server_id, :a@host -> String.starts_with?(server_id, "active_") end

    # Rewritten entries count toward the batch, so a batch of active sessions ends.
    Homes.evict_idle(homes, 2_500, 3, active?)
    assert Homes.size(homes) == 5
    Homes.evict_idle(homes, 2_500, 3, active?)
    assert Homes.size(homes) == 4
    assert Homes.home(name, "ended") == nil

    # Active entries are now dated at the eviction time and stay until they idle again.
    assert Enum.all?(Homes.entries(homes), fn {_server_id, :a@host, at} -> at == 2_500 end)
    Homes.evict_idle(homes, 3_500, 10, active?)
    assert Homes.size(homes) == 4
    # A newer replicated write still wins over a rewrite.
    assert Homes.put(homes, "active_1", :b@host, 2_501) == :updated
  end

  defp inactive(_server_id, _home), do: false
end
