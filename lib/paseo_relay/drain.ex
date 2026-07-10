defmodule PaseoRelay.Drain do
  @moduledoc """
  Process-local admission state for graceful relay maintenance.

  Starting a drain makes readiness fail immediately. Existing relay sessions are
  intentionally left to the protocol/routing layer, which can close them after
  its own grace period.
  """
  use Agent

  def start_link(initially_draining) do
    Agent.start_link(fn -> initially_draining end, name: __MODULE__)
  end

  def begin, do: Agent.update(__MODULE__, fn _ -> true end)
  def cancel, do: Agent.update(__MODULE__, fn _ -> false end)
  def draining?, do: Agent.get(__MODULE__, & &1)
end
