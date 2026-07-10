defmodule PaseoRelay.Ownership do
  @moduledoc """
  Distributed ownership for public sessions.

  Callers either proceed on the local owner node, or receive an opaque target
  that a deployment adapter can turn into a reroute response.
  """

  def claim(server_id, target), do: claim(server_id, target, self())

  def claim(server_id, target, session_owner) do
    :global.trans({__MODULE__, server_id}, fn ->
      case PaseoRelay.Ownership.Owner.start(server_id, target, session_owner) do
        {:ok, owner} ->
          :global.sync()

          if owner_pid(server_id) == owner do
            :local
          else
            Process.exit(owner, :normal)
            resolve(server_id)
          end

        {:error, :already_owned} ->
          resolve(server_id)
      end
    end)
  end

  def resolve(server_id) do
    :global.sync()

    case :global.whereis_name(name(server_id)) do
      :undefined -> :unowned
      owner when node(owner) == node() -> :local
      owner -> {:reroute, PaseoRelay.Ownership.Owner.target(owner)}
    end
  end

  def owner_pid(server_id), do: :global.whereis_name(name(server_id))
  def target(server_id), do: PaseoRelay.Ownership.Owner.target(owner_pid(server_id))
  def name(server_id), do: {__MODULE__, server_id}
end

defmodule PaseoRelay.Ownership.Owner do
  @moduledoc false
  use GenServer

  def start(server_id, target, session_owner),
    do: GenServer.start(__MODULE__, {server_id, target, session_owner})

  def target(owner), do: GenServer.call(owner, :target)

  @impl true
  def init({server_id, target, session_owner}) do
    case :global.register_name(PaseoRelay.Ownership.name(server_id), self()) do
      :yes -> {:ok, %{target: target, session: Process.monitor(session_owner)}}
      :no -> {:stop, :already_owned}
    end
  end

  @impl true
  def handle_call(:target, _from, state), do: {:reply, state.target, state}

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %{session: ref} = state),
    do: {:stop, :normal, state}
end
