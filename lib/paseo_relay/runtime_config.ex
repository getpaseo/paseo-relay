defmodule PaseoRelay.RuntimeConfig do
  @moduledoc """
  The settings of the running node. An operator changes them without a restart,
  for example over RPC:

      PaseoRelay.RuntimeConfig.put(%{connections_per_acceptor: 800})
      PaseoRelay.RuntimeConfig.get()

  Every `PaseoRelay.Config` field is live unless `Config.boot_only/0` lists it.
  `put/1` validates with the rules of the environment settings, then applies
  the change to Capacity, to the listener and to the open sockets. Open
  WebSockets stay connected. When Capacity does not take the change, nothing
  changes and `put/1` returns an error.

  Readers call `current/0`, which reads a persistent term and takes no lock.
  They must not keep the returned struct in their state: a `put/1` replaces the
  term, and the VM then copies it into each process that still holds it.
  """

  alias PaseoRelay.Config

  @doc "Loads the settings of the application environment when the node boots."
  @spec boot() :: Config.t()
  def boot do
    config = stored()
    :persistent_term.put(__MODULE__, config)
    config
  end

  @doc "Returns all settings that the node applies now."
  @spec current() :: Config.t()
  def current, do: :persistent_term.get(__MODULE__, nil) || stored()

  @doc "Returns the live settings."
  @spec get() :: map()
  def get, do: Map.take(current(), Config.live_keys())

  @doc """
  Validates and applies the given live settings. Returns all live settings, or
  an error when a value is invalid or Capacity is unavailable.
  """
  @spec put(map() | keyword()) :: {:ok, map()} | {:error, String.t()}
  def put(settings) when is_map(settings) or is_list(settings) do
    # One change at a time, so that two callers cannot lose each other's change.
    :global.trans({__MODULE__, self()}, fn -> change(Map.new(settings)) end, [node()])
  end

  defp change(settings) do
    previous = current()

    with {:ok, config} <- Config.put_live(previous, settings),
         :ok <- configure_capacity(previous, config) do
      :persistent_term.put(__MODULE__, config)
      Application.put_env(:paseo_relay, :runtime, config)
      PaseoRelay.Listener.reconfigure(PaseoRelay.Listener, previous, config)
      {:ok, Map.take(config, Config.live_keys())}
    end
  end

  # Capacity takes its settings before they are stored. When it does not answer,
  # it is killed, and its replacement reads the stored settings, which are still
  # the previous ones.
  defp configure_capacity(previous, config) do
    case PaseoRelay.Capacity.reconfigure(previous, config) do
      :ok -> :ok
      {:error, :unavailable} -> {:error, "Capacity is unavailable; nothing changed"}
    end
  end

  defp stored do
    :paseo_relay
    |> Application.get_env(:runtime, Config.defaults())
    |> Config.normalize()
  end
end
