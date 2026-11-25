defmodule PaseoRelay.Placement.Sessions do
  @moduledoc false

  # Index of the session Owners on this node: {server_id, owner, client_count}.
  # Owners write their own entry, so a heal scan never calls an Owner. This
  # process only owns the table, so the index survives a Placement.Server
  # restart. An Owner that is killed cannot remove its entry; `fold/2` drops
  # entries whose Owner is no longer alive.

  use GenServer

  @table :paseo_relay_local_sessions

  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def track(server_id, owner, client_count) do
    :ets.insert(@table, {server_id, owner, client_count})
    :ok
  rescue
    ArgumentError -> :ok
  end

  def untrack(server_id, owner) do
    :ets.select_delete(@table, [{{server_id, owner, :_}, [], [true]}])
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Folds `fun.({server_id, owner, client_count}, acc)` over the live sessions
  without copying the table.
  """
  def fold(acc, fun) do
    {acc, dead} =
      :ets.foldl(
        fn {_server_id, owner, _clients} = session, {acc, dead} ->
          if Process.alive?(owner), do: {fun.(session, acc), dead}, else: {acc, [session | dead]}
        end,
        {acc, []},
        @table
      )

    Enum.each(dead, fn {server_id, owner, _clients} -> untrack(server_id, owner) end)
    acc
  rescue
    ArgumentError -> acc
  end

  @impl true
  def init(:ok) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, nil}
  end
end
