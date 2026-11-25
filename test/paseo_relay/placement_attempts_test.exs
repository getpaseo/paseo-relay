defmodule PaseoRelay.PlacementAttemptsTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.Placement.Attempts

  @window 120_000
  @cooldown 1_800_000

  # Monotonic time starts near this value on a 64-bit BEAM.
  @start -576_460_752_303

  test "a heal attempt is forgotten one cooldown after its window" do
    attempts = Attempts.new(@window, @cooldown) |> Attempts.healed("s", @start)

    assert Attempts.size(Attempts.prune(attempts, @start + @window + @cooldown)) == 1
    assert Attempts.size(Attempts.prune(attempts, @start + @window + @cooldown + 1)) == 0
  end

  test "a first boomerang is forgotten one cooldown after its cooldown, at negative times" do
    returned_at = @start + 1_000

    assert {:cooldown, attempts} =
             Attempts.new(@window, @cooldown)
             |> Attempts.healed("s", @start)
             |> Attempts.returned("s", returned_at)

    refute Attempts.may_heal?(attempts, "s", returned_at + @cooldown - 1)
    assert Attempts.may_heal?(attempts, "s", returned_at + @cooldown)

    forgotten_at = returned_at + 2 * @cooldown + 1
    assert Attempts.size(Attempts.prune(attempts, forgotten_at - 1)) == 1
    assert Attempts.size(Attempts.prune(attempts, forgotten_at)) == 0
  end

  test "a second boomerang promotes and a late return is ignored" do
    attempts = Attempts.new(@window, @cooldown) |> Attempts.healed("s", @start)
    assert {:ignored, ^attempts} = Attempts.returned(attempts, "s", @start + @window + 1)

    {:cooldown, attempts} = Attempts.returned(attempts, "s", @start + 1)
    healed_again_at = @start + 1 + @cooldown
    attempts = Attempts.healed(attempts, "s", healed_again_at)

    assert {:promote, attempts} = Attempts.returned(attempts, "s", healed_again_at + 1)
    assert Attempts.size(attempts) == 0
  end
end
