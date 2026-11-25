defmodule PaseoRelay.ConfigTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.Config

  test "loads generic release settings with safe local defaults" do
    assert Config.load([]) == {:ok, Config.defaults()}
  end

  test "leaves the receive buffer to the kernel and fixes the user-space buffer" do
    assert {:ok, %{tcp_receive_buffer_bytes: :auto, tcp_user_buffer_bytes: 65_536}} =
             Config.load([])

    for automatic <- ["auto", ""] do
      assert {:ok, %{tcp_receive_buffer_bytes: :auto}} =
               Config.load([{"PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", automatic}])
    end

    assert {:ok, %{tcp_receive_buffer_bytes: 1_048_576, tcp_user_buffer_bytes: 32_768}} =
             Config.load([
               {"PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", "1048576"},
               {"PASEO_RELAY_TCP_USER_BUFFER_BYTES", "32768"}
             ])

    assert Config.load([{"PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", "1048577"}]) ==
             {:error,
              "PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES must be auto or an integer between 4096 and 1048576"}

    assert Config.load([{"PASEO_RELAY_TCP_USER_BUFFER_BYTES", "4095"}]) ==
             {:error,
              "PASEO_RELAY_TCP_USER_BUFFER_BYTES must be an integer between 4096 and 1048576"}
  end

  test "admits 50,000 WebSockets by default" do
    assert %{acceptors: 100, connections_per_acceptor: 500} = Config.defaults()
    assert Config.max_websockets(Config.defaults()) == 50_000
  end

  test "loads and validates the OOM guard as percentages of total memory" do
    assert %{oom_min_available_percent: 10, oom_recovery_available_percent: 15} =
             Config.defaults()

    assert {:ok, %{oom_min_available_percent: 0}} =
             Config.load([{"PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT", "0"}])

    assert {:ok, %{oom_min_available_percent: 5, oom_recovery_available_percent: 8}} =
             Config.load([
               {"PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT", "5"},
               {"PASEO_RELAY_OOM_RECOVERY_AVAILABLE_PERCENT", "8"}
             ])

    assert Config.load([{"PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT", "51"}]) ==
             {:error, "PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT must be an integer between 0 and 50"}

    assert Config.load([{"PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT", "20"}]) ==
             {:error,
              "PASEO_RELAY_OOM_RECOVERY_AVAILABLE_PERCENT must not be lower than PASEO_RELAY_OOM_MIN_AVAILABLE_PERCENT"}
  end

  test "loads and validates the Capacity mutation timeout" do
    assert Config.defaults().capacity_mutation_timeout_ms == 5_000

    assert {:ok, %{capacity_mutation_timeout_ms: 7_500}} =
             Config.load([{"PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS", "7500"}])

    assert Config.load([{"PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS", "99"}]) ==
             {:error,
              "PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS must be an integer between 100 and 120000"}
  end

  test "rejects a listener hostname that the socket layer cannot bind" do
    assert Config.load([{"PASEO_RELAY_HOST", "not-an-ip"}]) ==
             {:error, "PASEO_RELAY_HOST must be an IP address"}
  end

  test "rejects an invalid port instead of starting on an unintended listener" do
    assert Config.load([{"PASEO_RELAY_PORT", "not-a-port"}]) ==
             {:error, "PASEO_RELAY_PORT must be an integer between 1 and 65535"}
  end

  test "recognizes drain mode from the release environment" do
    assert {:ok, %{drain: true, port: 4400}} =
             Config.load([{"PASEO_RELAY_DRAIN", "true"}, {"PASEO_RELAY_PORT", "4400"}])
  end

  test "requires the websocket heap fuse to admit a maximum legal frame" do
    assert Config.load([{"PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS", "33554431"}]) ==
             {:error,
              "PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS must be an integer between 33554432 and 134217728"}

    assert {:ok, %{websocket_max_heap_words: 33_554_432}} =
             Config.load([{"PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS", "33554432"}])
  end

  test "loads and validates the listener ceiling as connections per acceptor" do
    assert {:ok, %{acceptors: 20, connections_per_acceptor: 750, http_idle_timeout_ms: 10_000}} =
             Config.load([
               {"PASEO_RELAY_ACCEPTORS", "20"},
               {"PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", "750"},
               {"PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS", "10000"}
             ])

    assert Config.load([{"PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", "0"}]) ==
             {:error,
              "PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR must be an integer between 1 and 1000000"}

    assert Config.load([{"PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS", "0"}]) ==
             {:error,
              "PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS must be an integer between 100 and 120000"}
  end

  test "validates the weighted ingress envelope and delivery limits" do
    assert {:ok,
            %{
              ingress_budget_bytes: 256_000_000,
              ingress_weight: 2,
              delivery_timeout_ms: 5_000,
              transport_send_timeout_ms: 6_000,
              control_queue_bytes: 1_024,
              tcp_receive_buffer_bytes: 32_768
            }} =
             Config.load([
               {"PASEO_RELAY_INGRESS_BUDGET_BYTES", "256000000"},
               {"PASEO_RELAY_INGRESS_WEIGHT", "2"},
               {"PASEO_RELAY_DELIVERY_TIMEOUT_MS", "5000"},
               {"PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS", "6000"},
               {"PASEO_RELAY_CONTROL_QUEUE_BYTES", "1024"},
               {"PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", "32768"}
             ])

    assert Config.load([
             {"PASEO_RELAY_DELIVERY_TIMEOUT_MS", "5000"},
             {"PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS", "5000"}
           ]) ==
             {:error,
              "PASEO_RELAY_DELIVERY_TIMEOUT_MS must be lower than PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS"}

    assert Config.load([
             {"PASEO_RELAY_INGRESS_BUDGET_BYTES", Integer.to_string(128 * 1024 * 1024)},
             {"PASEO_RELAY_INGRESS_WEIGHT", "8"}
           ]) ==
             {:error,
              "PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message at the configured weight"}

    assert {:ok, %{memory_watermark_bytes: 0}} =
             Config.load([{"PASEO_RELAY_MEMORY_WATERMARK_BYTES", "0"}])

    assert {:ok, %{memory_watermark_bytes: 1_500_000_000}} =
             Config.load([{"PASEO_RELAY_MEMORY_WATERMARK_BYTES", "1500000000"}])
  end

  test "requires capacity for one maximum complete message" do
    weight = 5
    exact_budget = PaseoRelay.Protocol.maximum_message_payload_bytes() * weight

    assert {:ok, %{ingress_budget_bytes: ^exact_budget, ingress_weight: ^weight}} =
             Config.load([
               {"PASEO_RELAY_INGRESS_BUDGET_BYTES", Integer.to_string(exact_budget)},
               {"PASEO_RELAY_INGRESS_WEIGHT", Integer.to_string(weight)}
             ])

    assert Config.load([
             {"PASEO_RELAY_INGRESS_BUDGET_BYTES", Integer.to_string(exact_budget - 1)},
             {"PASEO_RELAY_INGRESS_WEIGHT", Integer.to_string(weight)}
           ]) ==
             {:error,
              "PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message at the configured weight"}
  end

  test "loads and validates the session-healing settings" do
    assert %{heal_enabled: true, heal_delay_ms: 360_000, heal_rate_per_sec: 25} =
             Config.defaults()

    assert {:ok, %{heal_enabled: false, heal_delay_ms: 600_000, heal_rate_per_sec: 500}} =
             Config.load([
               {"PASEO_RELAY_HEAL_ENABLED", "false"},
               {"PASEO_RELAY_HEAL_DELAY_MS", "600000"},
               {"PASEO_RELAY_HEAL_RATE_PER_SEC", "500"}
             ])

    assert Config.load([{"PASEO_RELAY_HEAL_ENABLED", "yes"}]) ==
             {:error, "PASEO_RELAY_HEAL_ENABLED must be true or false"}

    assert Config.load([{"PASEO_RELAY_HEAL_DELAY_MS", "-1"}]) ==
             {:error, "PASEO_RELAY_HEAL_DELAY_MS must be an integer between 0 and 86400000"}

    assert Config.load([{"PASEO_RELAY_HEAL_RATE_PER_SEC", "0"}]) ==
             {:error, "PASEO_RELAY_HEAL_RATE_PER_SEC must be an integer between 1 and 500"}

    assert Config.load([{"PASEO_RELAY_HEAL_RATE_PER_SEC", "501"}]) ==
             {:error, "PASEO_RELAY_HEAL_RATE_PER_SEC must be an integer between 1 and 500"}
  end

  test "loads and validates the heal attempt window, the cooldown and the home idle limit" do
    assert %{
             heal_attempt_window_ms: 120_000,
             heal_cooldown_ms: 1_800_000,
             home_max_idle_ms: 604_800_000
           } = Config.defaults()

    assert {:ok,
            %{heal_attempt_window_ms: 60_000, heal_cooldown_ms: 0, home_max_idle_ms: 86_400_000}} =
             Config.load([
               {"PASEO_RELAY_HEAL_ATTEMPT_WINDOW_MS", "60000"},
               {"PASEO_RELAY_HEAL_COOLDOWN_MS", "0"},
               {"PASEO_RELAY_HOME_MAX_IDLE_MS", "86400000"}
             ])

    assert Config.load([{"PASEO_RELAY_HEAL_ATTEMPT_WINDOW_MS", "999"}]) ==
             {:error,
              "PASEO_RELAY_HEAL_ATTEMPT_WINDOW_MS must be an integer between 1000 and 86400000"}

    assert Config.load([{"PASEO_RELAY_HOME_MAX_IDLE_MS", "999"}]) ==
             {:error,
              "PASEO_RELAY_HOME_MAX_IDLE_MS must be an integer between 1000 and 7776000000"}
  end
end
