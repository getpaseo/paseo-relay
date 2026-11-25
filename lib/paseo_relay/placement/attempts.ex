defmodule PaseoRelay.Placement.Attempts do
  @moduledoc false

  # Heal attempts on this node, keyed by server_id. Times are monotonic
  # milliseconds, which are negative on a 64-bit BEAM, so no rule may treat a
  # missing time as 0.
  #
  # A daemon that comes straight back after a heal is a "boomerang": its
  # network prefers this node. The first boomerang within `window_ms` of a heal
  # starts a cooldown of `cooldown_ms`; the second one promotes this node to
  # the session's home.

  @enforce_keys [:window_ms, :cooldown_ms]
  defstruct [:window_ms, :cooldown_ms, by_server: %{}]

  def new(window_ms, cooldown_ms), do: %__MODULE__{window_ms: window_ms, cooldown_ms: cooldown_ms}

  @doc "Returns whether `server_id` may be healed at `now`."
  def may_heal?(%__MODULE__{} = attempts, server_id, now) do
    case attempts.by_server[server_id] do
      nil ->
        true

      attempt ->
        now >= attempt.cooldown_until and
          (attempt.healed_at == nil or now - attempt.healed_at > attempts.window_ms)
    end
  end

  @doc "Records a heal of `server_id` at `now`."
  def healed(%__MODULE__{} = attempts, server_id, now) do
    attempt =
      attempts.by_server
      |> Map.get(server_id, %{boomerangs: 0, cooldown_until: now})
      |> Map.put(:healed_at, now)

    put(attempts, server_id, attempt)
  end

  @doc """
  Records a direct return of `server_id` at `now`. Returns `:ignored` when no
  heal is recent, `:cooldown` for the first boomerang and `:promote` for the
  second.
  """
  def returned(%__MODULE__{} = attempts, server_id, now) do
    case attempts.by_server[server_id] do
      %{healed_at: healed_at} = attempt
      when is_integer(healed_at) and now - healed_at <= attempts.window_ms ->
        boomerang(attempts, server_id, attempt, now)

      _no_recent_heal ->
        {:ignored, attempts}
    end
  end

  def forget(%__MODULE__{} = attempts, server_id),
    do: %{attempts | by_server: Map.delete(attempts.by_server, server_id)}

  @doc """
  Forgets attempts that have no effect left. An attempt is kept for one
  cooldown after its last effect, so a boomerang count survives until the
  session can be healed again.
  """
  def prune(%__MODULE__{} = attempts, now) do
    by_server =
      Map.filter(attempts.by_server, fn {_server_id, attempt} ->
        now <= last_effect(attempts, attempt) + attempts.cooldown_ms
      end)

    %{attempts | by_server: by_server}
  end

  def size(%__MODULE__{by_server: by_server}), do: map_size(by_server)

  # A first boomerang clears `healed_at`; its cooldown is then the last effect.
  defp last_effect(_attempts, %{healed_at: nil, cooldown_until: cooldown_until}),
    do: cooldown_until

  defp last_effect(attempts, %{healed_at: healed_at, cooldown_until: cooldown_until}),
    do: max(healed_at + attempts.window_ms, cooldown_until)

  defp boomerang(attempts, server_id, %{boomerangs: 0} = attempt, now) do
    cooldown_until = now + attempts.cooldown_ms
    attempt = %{attempt | healed_at: nil, boomerangs: 1, cooldown_until: cooldown_until}
    {:cooldown, put(attempts, server_id, attempt)}
  end

  defp boomerang(attempts, server_id, _attempt, _now),
    do: {:promote, forget(attempts, server_id)}

  defp put(attempts, server_id, attempt),
    do: %{attempts | by_server: Map.put(attempts.by_server, server_id, attempt)}
end
