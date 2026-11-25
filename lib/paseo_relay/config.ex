defmodule PaseoRelay.Config do
  @moduledoc """
  The relay settings: one table gives each field its default, its environment
  variable and its validation rule.

  `load/1` reads the table from the environment when the node boots.
  `put_live/2` applies the same rules to a change on a running node. A field
  can change live unless it is listed in `boot_only/0`, so a field added to the
  table is a live setting without more work.
  """

  @mebibyte 1024 * 1024
  @day_ms 24 * 60 * 60 * 1_000

  # {field, default, environment variable, rule}
  @settings [
    {:host, "127.0.0.1", "PASEO_RELAY_HOST", :ip_address},
    {:port, 4000, "PASEO_RELAY_PORT", {:integer, 1..65_535}},
    {:drain, false, "PASEO_RELAY_DRAIN", :boolean},
    {:acceptors, 100, "PASEO_RELAY_ACCEPTORS", {:integer, 1..1_000}},
    {:connections_per_acceptor, 500, "PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR",
     {:integer, 1..1_000_000}},
    {:http_idle_timeout_ms, 15_000, "PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS", {:integer, 100..120_000}},
    {:capacity_mutation_timeout_ms, 5_000, "PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS",
     {:integer, 100..120_000}},
    {:ingress_budget_bytes, 512 * @mebibyte, "PASEO_RELAY_INGRESS_BUDGET_BYTES",
     {:integer, (128 * @mebibyte)..(8 * 1024 * @mebibyte)}},
    {:ingress_weight, 4, "PASEO_RELAY_INGRESS_WEIGHT", {:integer, 1..16}},
    {:delivery_timeout_ms, 30_000, "PASEO_RELAY_DELIVERY_TIMEOUT_MS", {:integer, 100..120_000}},
    {:transport_send_timeout_ms, 35_000, "PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS",
     {:integer, 100..300_000}},
    {:control_queue_bytes, @mebibyte, "PASEO_RELAY_CONTROL_QUEUE_BYTES",
     {:integer, 64..(64 * @mebibyte)}},
    {:data_attach_timeout_ms, 15_000, "PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS",
     {:integer, 1_000..120_000}},
    {:tcp_user_buffer_bytes, 65_536, "PASEO_RELAY_TCP_USER_BUFFER_BYTES",
     {:integer, 4_096..@mebibyte}},
    {:tcp_receive_buffer_bytes, :auto, "PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES",
     {:auto_or_integer, 4_096..@mebibyte}},
    {:websocket_max_heap_words, 32 * @mebibyte, "PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS",
     {:integer, (32 * @mebibyte)..(128 * @mebibyte)}},
    {:memory_watermark_bytes, 0, "PASEO_RELAY_MEMORY_WATERMARK_BYTES",
     {:zero_or_integer, (256 * @mebibyte)..(64 * 1024 * @mebibyte)}},
    {:oom_min_available_percent, 10, "PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT", {:integer, 0..50}},
    {:oom_recovery_available_percent, 15, "PASEO_RELAY_OOM_RECOVERY_AVAILABLE_PERCENT",
     {:integer, 0..100}},
    {:heal_enabled, true, "PASEO_RELAY_HEAL_ENABLED", :boolean},
    {:heal_delay_ms, 360_000, "PASEO_RELAY_HEAL_DELAY_MS", {:integer, 0..@day_ms}},
    {:heal_rate_per_sec, 25, "PASEO_RELAY_HEAL_RATE_PER_SEC", {:integer, 1..500}},
    {:heal_attempt_window_ms, 120_000, "PASEO_RELAY_HEAL_ATTEMPT_WINDOW_MS",
     {:integer, 1_000..@day_ms}},
    {:heal_cooldown_ms, 1_800_000, "PASEO_RELAY_HEAL_COOLDOWN_MS", {:integer, 0..(7 * @day_ms)}},
    {:home_max_idle_ms, 7 * @day_ms, "PASEO_RELAY_HOME_MAX_IDLE_MS",
     {:integer, 1_000..(90 * @day_ms)}},
    {:ownership_target, "local", "PASEO_RELAY_OWNERSHIP_TARGET", :string},
    {:reroute_header, "x-reroute-target", "PASEO_RELAY_REROUTE_HEADER", :string},
    {:minimum_cluster_size, 1, "PASEO_RELAY_MIN_CLUSTER_SIZE", {:integer, 1..1_000}},
    {:cluster_query, nil, "PASEO_RELAY_CLUSTER_QUERY", :optional_string},
    {:cluster_nodes, [], "PASEO_RELAY_CLUSTER_NODES", :nodes},
    {:node_name, nil, "RELEASE_NODE", :optional_string},
    {:cookie, nil, "RELEASE_COOKIE", :optional_string}
  ]

  # The listener address, the acceptor pool and the node identity exist once per
  # VM. `ip` is the parsed `host`. DNSCluster takes its query only when it starts.
  @boot_only [:host, :ip, :port, :acceptors, :cluster_query, :node_name, :cookie]

  defstruct [
    {:ip, {127, 0, 0, 1}} | Enum.map(@settings, fn {field, default, _, _} -> {field, default} end)
  ]

  @type t :: %__MODULE__{}

  @spec load(Enumerable.t()) :: {:ok, t()} | {:error, String.t()}
  def load(environment \\ System.get_env()) do
    environment = Map.new(environment)

    with {:ok, config} <- read_environment(environment),
         :ok <- validate_relations(config, &variable/1) do
      {:ok, ip} = :inet.parse_address(String.to_charlist(config.host))
      {:ok, %{config | ip: ip}}
    end
  end

  @spec defaults() :: t()
  def defaults, do: %__MODULE__{}

  @spec normalize(t() | map() | keyword()) :: t()
  def normalize(%__MODULE__{} = config), do: config
  def normalize(settings), do: struct!(__MODULE__, Map.new(settings))

  @doc "The fields that are fixed when the node boots."
  @spec boot_only() :: [atom()]
  def boot_only, do: @boot_only

  @doc "The fields that can change while the node runs: every field that is not boot-only."
  @spec live_keys() :: [atom()]
  def live_keys, do: for({field, _, _, _} <- @settings, field not in @boot_only, do: field)

  @doc """
  Returns `config` with the live `settings` applied.

  The rules match the environment rules for the same settings. A boot-only or
  unknown key, a value of the wrong type and a combination that the environment
  would reject are errors.
  """
  @spec put_live(t(), map()) :: {:ok, t()} | {:error, String.t()}
  def put_live(%__MODULE__{} = config, settings) when is_map(settings) do
    with :ok <- validate_live(settings),
         config = struct!(config, settings),
         :ok <- validate_relations(config, &Atom.to_string/1) do
      {:ok, config}
    end
  end

  @doc "The node-local ceiling for open WebSockets."
  @spec max_websockets(t()) :: pos_integer()
  def max_websockets(%__MODULE__{} = config),
    do: config.acceptors * config.connections_per_acceptor

  defp read_environment(environment) do
    Enum.reduce_while(@settings, {:ok, defaults()}, fn {field, _default, variable, rule},
                                                       {:ok, config} ->
      with {:ok, text} <- Map.fetch(environment, variable),
           {:ok, value} <- parse(rule, text),
           true <- valid?(rule, value) do
        {:cont, {:ok, Map.put(config, field, value)}}
      else
        :error when not is_map_key(environment, variable) -> {:cont, {:ok, config}}
        _invalid -> {:halt, {:error, "#{variable} #{requirement(rule)}"}}
      end
    end)
  end

  defp validate_live(settings) do
    live = live_keys()

    Enum.find_value(settings, :ok, fn {key, value} ->
      cond do
        key not in live -> {:error, "#{inspect(key)} is not a live setting"}
        valid?(rule(key), value) -> nil
        true -> {:error, "#{key} #{requirement(rule(key))}"}
      end
    end)
  end

  # Rules between settings. `name` gives each field its environment or live name.
  defp validate_relations(config, name) do
    cond do
      config.ingress_budget_bytes <
          PaseoRelay.Protocol.maximum_message_payload_bytes() * config.ingress_weight ->
        {:error,
         "#{name.(:ingress_budget_bytes)} must admit one maximum assembled message at the configured weight"}

      config.delivery_timeout_ms >= config.transport_send_timeout_ms ->
        {:error,
         "#{name.(:delivery_timeout_ms)} must be lower than #{name.(:transport_send_timeout_ms)}"}

      config.oom_min_available_percent > config.oom_recovery_available_percent ->
        {:error,
         "#{name.(:oom_recovery_available_percent)} must not be lower than #{name.(:oom_min_available_percent)}"}

      true ->
        :ok
    end
  end

  for {field, _default, variable, rule} <- @settings do
    defp variable(unquote(field)), do: unquote(variable)
    defp rule(unquote(field)), do: unquote(Macro.escape(rule))
  end

  defp parse({:integer, _range}, text), do: parse_integer(text)
  defp parse({:zero_or_integer, _range}, text), do: parse_integer(text)
  defp parse({:auto_or_integer, _range}, text) when text in ["", "auto"], do: {:ok, :auto}
  defp parse({:auto_or_integer, _range}, text), do: parse_integer(text)
  defp parse(:boolean, "true"), do: {:ok, true}
  defp parse(:boolean, "false"), do: {:ok, false}
  defp parse(:boolean, _text), do: :error

  defp parse(:nodes, text) do
    names = text |> String.split(",", trim: true) |> Enum.map(&String.trim/1) |> Enum.uniq()

    # Checked before the names become atoms.
    if node_names?(names), do: {:ok, Enum.map(names, &String.to_atom/1)}, else: :error
  end

  defp parse(_text_rule, text), do: {:ok, text}

  defp parse_integer(text) do
    case Integer.parse(text) do
      {integer, ""} -> {:ok, integer}
      _not_an_integer -> :error
    end
  end

  defp valid?({:integer, range}, value), do: is_integer(value) and value in range

  defp valid?({:zero_or_integer, range}, value),
    do: value == 0 or valid?({:integer, range}, value)

  defp valid?({:auto_or_integer, range}, value),
    do: value == :auto or valid?({:integer, range}, value)

  defp valid?(:boolean, value), do: is_boolean(value)
  defp valid?(:string, value), do: is_binary(value)
  defp valid?(:optional_string, value), do: value == nil or is_binary(value)

  defp valid?(:ip_address, value),
    do: is_binary(value) and match?({:ok, _ip}, :inet.parse_address(String.to_charlist(value)))

  defp valid?(:nodes, value),
    do:
      is_list(value) and Enum.all?(value, &is_atom/1) and
        node_names?(Enum.map(value, &Atom.to_string/1))

  defp node_names?(names) do
    length(names) <= 1_000 and
      Enum.all?(names, &(byte_size(&1) <= 255 and Regex.match?(~r/^[^\s@,]+@[^\s@,]+$/, &1)))
  end

  defp requirement({:integer, range}),
    do: "must be an integer between #{range.first} and #{range.last}"

  defp requirement({:zero_or_integer, range}), do: "must be 0 or #{between(range)}"
  defp requirement({:auto_or_integer, range}), do: "must be auto or #{between(range)}"
  defp requirement(:boolean), do: "must be true or false"
  defp requirement(:string), do: "must be a string"
  defp requirement(:optional_string), do: "must be a string"
  defp requirement(:ip_address), do: "must be an IP address"
  defp requirement(:nodes), do: "must contain at most 1000 comma-separated name@host peers"

  defp between(range), do: "an integer between #{range.first} and #{range.last}"
end
