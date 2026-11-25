defmodule PaseoRelay.RuntimeConfig do
  @moduledoc """
  Settings an operator can change on a running node, for example over RPC:

      PaseoRelay.RuntimeConfig.put(%{memory_watermark_bytes: 5_368_709_120})
      PaseoRelay.RuntimeConfig.get()

  `put/1` validates with the same rules as the environment settings, applies
  the values to Capacity and Placement and stores them in the application
  environment. When Capacity or Placement does not answer, nothing changes and
  `put/1` returns an error. Existing WebSockets stay open. When Capacity or
  Placement restarts under its supervisor, it starts with the stored values.
  """

  alias PaseoRelay.Config

  @heal_keys [:heal_enabled, :heal_delay_ms, :heal_rate_per_sec]
  @keys [:memory_watermark_bytes | @heal_keys]

  @type settings :: %{
          optional(:memory_watermark_bytes) => non_neg_integer(),
          optional(:heal_enabled) => boolean(),
          optional(:heal_delay_ms) => non_neg_integer(),
          optional(:heal_rate_per_sec) => pos_integer()
        }

  @doc "Returns the stored live settings."
  @spec get() :: settings()
  def get, do: Map.take(stored(), @keys)

  @doc """
  Validates, applies and stores the given live settings. Returns an error and
  keeps the previous settings when Capacity or Placement cannot apply them.
  """
  @spec put(settings() | keyword()) :: {:ok, settings()} | {:error, String.t()}
  def put(settings) when is_map(settings) or is_list(settings) do
    with {:ok, settings} <- Config.validate_live(Map.new(settings)) do
      previous = stored()
      config = struct!(previous, settings)

      with :ok <- apply_watermark(settings, config),
           :ok <- store_heal_settings(settings, previous, config) do
        {:ok, Map.take(config, @keys)}
      end
    end
  end

  @doc "Returns `config` with the stored live settings applied."
  @spec overlay(Config.t()) :: Config.t()
  def overlay(%Config{} = config), do: struct!(config, get())

  # Capacity takes the watermark before it is stored. When Capacity does not
  # answer, it is restarted and reads the stored watermark, which is still the
  # previous one.
  defp apply_watermark(settings, config) do
    if Map.has_key?(settings, :memory_watermark_bytes) and set_watermark(config) != :ok,
      do: {:error, "Capacity is unavailable; nothing changed"},
      else: :ok
  end

  # Placement reads the stored settings when it handles a reload. After a
  # rollback, a reload that it handles late therefore applies the previous
  # settings, and the extra reload covers one handled during the rollback.
  defp store_heal_settings(settings, previous, config) do
    Application.put_env(:paseo_relay, :runtime, config)

    if Enum.any?(@heal_keys, &Map.has_key?(settings, &1)) and
         PaseoRelay.Placement.reload_settings() != :ok do
      Application.put_env(:paseo_relay, :runtime, previous)
      PaseoRelay.Placement.request_reload()
      if Map.has_key?(settings, :memory_watermark_bytes), do: set_watermark(previous)
      {:error, "Placement is unavailable; nothing changed"}
    else
      :ok
    end
  end

  defp set_watermark(config) do
    PaseoRelay.Capacity.set_watermark(
      config.memory_watermark_bytes,
      config.capacity_mutation_timeout_ms
    )
  end

  defp stored do
    :paseo_relay
    |> Application.get_env(:runtime, Config.defaults())
    |> Config.normalize()
  end
end
