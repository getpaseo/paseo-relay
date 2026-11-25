defmodule PaseoRelay.Placement do
  @moduledoc """
  Session healing: returns a session to its home node after it fails over.

  A session's home is the node where its daemon control connection arrives
  directly, without a reroute. Each node keeps a bounded, replicated map of
  homes and a local index of the sessions it owns. When a home node has been
  ready for `heal_delay_ms`, the node that owns a session from that home stops
  the session Owner. The daemon then reconnects and claims its home again.

  Callers use only this module. `Owner` reports its lifecycle and client count,
  `Socket` reports direct daemon arrivals, and `RuntimeConfig` changes the heal
  settings.
  """

  alias PaseoRelay.Placement.Homes
  alias PaseoRelay.Placement.Server
  alias PaseoRelay.Placement.Sessions

  def child_spec(config) do
    %{id: __MODULE__, type: :supervisor, start: {__MODULE__, :start_link, [config]}}
  end

  def start_link(config) do
    Supervisor.start_link([Sessions, {Server, config}],
      strategy: :one_for_one,
      name: PaseoRelay.Placement.Supervisor
    )
  end

  @doc "Records that a daemon control connection arrived at this node without a reroute."
  def daemon_arrived(server_id), do: Server.daemon_arrived(server_id)

  @doc "Returns the home node of `server_id`, or `nil` when no home is known."
  def home(server_id), do: Homes.home(Server.homes_table(), server_id)

  @doc "Counts the sessions this node owns whose home is another node."
  def misplaced_sessions do
    local = node()

    Sessions.fold(0, fn {server_id, _owner, _clients}, count ->
      if home(server_id) in [nil, local], do: count, else: count + 1
    end)
  end

  @doc "Returns the heal settings that this node applies now."
  def settings, do: Server.settings()

  @doc """
  Applies the stored heal settings from `RuntimeConfig`. Returns `:ok`, or
  `{:error, :unavailable}` when Placement did not answer in time.
  """
  def reload_settings, do: Server.reload_settings()

  @doc "Asks Placement to apply the stored heal settings, without waiting."
  def request_reload, do: Server.request_reload()

  # Owner lifecycle. Each call writes the calling Owner's entry.
  def session_started(server_id), do: Sessions.track(server_id, self(), 0)
  def session_clients(server_id, count), do: Sessions.track(server_id, self(), count)
  def session_stopped(server_id), do: Sessions.untrack(server_id, self())
end
