#!/usr/bin/env bash
set -euo pipefail

readonly fly_config="deployment/fly/fly.toml"

fly_dockerfile_from_config() {
  awk '
    /^\[build\]$/ { in_build = 1; next }
    /^\[/ { in_build = 0 }
    in_build && /^[[:space:]]*dockerfile[[:space:]]*=/ {
      value = $0
      sub(/^[^=]*=[[:space:]]*"/, "", value)
      sub(/"[[:space:]]*$/, "", value)
      print value
      exit
    }
  ' "${fly_config}"
}

validate_fly_build() {
  if [[ -z "${fly_dockerfile}" ]]; then
    echo "${fly_config} must declare [build] dockerfile" >&2
    return 1
  fi

  if [[ ! -f "${fly_dockerfile}" ]]; then
    echo "Fly Dockerfile selected by ${fly_config} does not exist: ${fly_dockerfile}" >&2
    return 1
  fi
}

fly_dockerfile="$(fly_dockerfile_from_config)"
if [[ "${fly_dockerfile}" != /* ]]; then
  fly_dockerfile="$(dirname "${fly_config}")/${fly_dockerfile}"
fi
readonly fly_dockerfile
validate_fly_build

if [[ "${1:-}" == "--validate-fly-build" ]]; then
  echo "Fly adapter Dockerfile: ${fly_dockerfile}"
  exit 0
fi

readonly release_port="${PASEO_RELAY_CI_RELEASE_PORT:-4400}"
readonly container_port="${PASEO_RELAY_CI_CONTAINER_PORT:-4401}"
readonly fly_port="${PASEO_RELAY_CI_FLY_PORT:-4402}"
readonly run_id="${GITHUB_RUN_ID:-local}-$$"
readonly generic_image="paseo-relay-ci-generic:${run_id}"
readonly fly_image="paseo-relay-ci-fly:${run_id}"
readonly load_image="paseo-relay-ci-load:${run_id}"
readonly generic_container="paseo-relay-ci-generic-${run_id}"
readonly fly_container="paseo-relay-ci-fly-${run_id}"

release_pid=""
mesh_caddy_config=""

cleanup() {
  if [[ -n "${release_pid}" ]] && kill -0 "${release_pid}" 2>/dev/null; then
    kill "${release_pid}"
    wait "${release_pid}" || true
  fi

  docker rm --force "${generic_container}" "${fly_container}" >/dev/null 2>&1 || true
  if [[ -n "${mesh_caddy_config}" ]]; then rm -f "${mesh_caddy_config}"; fi
}

trap cleanup EXIT

wait_for_endpoint() {
  local url="$1"

  for _attempt in $(seq 1 100); do
    if curl --fail --silent --show-error "${url}" >/dev/null 2>&1; then
      return 0
    fi

    sleep 0.1
  done

  echo "endpoint did not become ready: ${url}" >&2
  return 1
}

assert_operations_contract() {
  local base_url="$1"
  local metrics

  [[ "$(curl --fail --silent --show-error "${base_url}/health")" == '{"status":"ok"}' ]]
  [[ "$(curl --fail --silent --show-error "${base_url}/ready")" == '{"status":"ready"}' ]]
  metrics="$(curl --fail --silent --show-error "${base_url}/metrics")"
  grep --quiet '^paseo_relay_ready 1$' <<<"${metrics}"
}

print_bounded_rpc_output() {
  local label="$1"
  local output="$2"

  echo "${label} (last 4096 bytes):" >&2
  printf '%s\n' "${output}" | tail -c 4096 >&2
}

mix deps.get
mix hex.audit
mix format --check-formatted
env MIX_ENV=test mix compile --warnings-as-errors
mix deps.unlock --check-unused
mix test
env MIX_ENV=prod mix compile --warnings-as-errors
env MIX_ENV=prod mix release --overwrite

PASEO_RELAY_HOST=127.0.0.1 \
PASEO_RELAY_PORT="${release_port}" \
PASEO_RELAY_MIN_CLUSTER_SIZE=1 \
  _build/prod/rel/paseo_relay/bin/paseo_relay start &
release_pid=$!
wait_for_endpoint "http://127.0.0.1:${release_port}/health"
assert_operations_contract "http://127.0.0.1:${release_port}"
kill "${release_pid}"
wait "${release_pid}" || true
release_pid=""

docker build --tag "${generic_image}" .
docker build --file "${fly_dockerfile}" --tag "${fly_image}" .
docker build --file deployment/load/Dockerfile --tag "${load_image}" .

if [[ "$(docker image inspect --format '{{json .Config.Entrypoint}}' "${fly_image}")" != \
  '["/adapter-entrypoint"]' ]]; then
  echo "Fly image does not use /adapter-entrypoint" >&2
  exit 1
fi

docker run --detach --name "${generic_container}" \
  --publish "127.0.0.1:${container_port}:4000" \
  "${generic_image}" >/dev/null
wait_for_endpoint "http://127.0.0.1:${container_port}/health"
assert_operations_contract "http://127.0.0.1:${container_port}"

node scripts/relay-load.mjs \
  --endpoints "ws://127.0.0.1:${container_port}/ws" \
  --scenario sustained --pairs 25 --batch-size 25 --rate 20 --duration 2 \
  --cleanup-grace 5 --drain-timeout 5

docker restart "${generic_container}" >/dev/null
if ! wait_for_endpoint "http://127.0.0.1:${container_port}/health"; then
  docker logs "${generic_container}" >&2
  exit 1
fi
assert_operations_contract "http://127.0.0.1:${container_port}"

node scripts/relay-load.mjs \
  --endpoints "ws://127.0.0.1:${container_port}/ws" \
  --scenario reconnect --pairs 25 --batch-size 25 --reconnects 2 --duration 1 \
  --cleanup-grace 5 --drain-timeout 5
node scripts/relay-load.mjs \
  --endpoints "ws://127.0.0.1:${container_port}/ws" \
  --scenario ownership --servers 200 --batch-size 100 --duration 0 \
  --cleanup-grace 5 --drain-timeout 5

docker run --detach --name "${fly_container}" \
  --ulimit nofile=100000:100000 \
  --env FLY_APP_NAME=paseo-relay-ci \
  --env FLY_MACHINE_ID=ci-machine \
  --env FLY_PRIVATE_IP=::1 \
  --env PASEO_RELAY_HOST=0.0.0.0 \
  --env PASEO_RELAY_PORT=4000 \
  --env PASEO_RELAY_CLUSTER_QUERY=ignore \
  --env PASEO_RELAY_MIN_CLUSTER_SIZE=1 \
  --publish "127.0.0.1:${fly_port}:4000" \
  "${fly_image}" >/dev/null
if ! wait_for_endpoint "http://127.0.0.1:${fly_port}/health"; then
  docker logs "${fly_container}" >&2
  exit 1
fi
assert_operations_contract "http://127.0.0.1:${fly_port}"

if ! fly_snapshot_output="$(docker exec "${fly_container}" sh -lc \
  'RELEASE_NODE="paseo_relay@$FLY_PRIVATE_IP" RELEASE_DISTRIBUTION=name ERL_AFLAGS="-proto_dist inet6_tcp" ELIXIR_ERL_OPTIONS="+fnu" /app/bin/paseo_relay rpc '\''PaseoRelay.FlyDiagnostics.print_snapshot("WyJjaS11bm93bmVkIl0")'\''' 2>&1)"; then
  print_bounded_rpc_output "Fly diagnostic RPC failed" "${fly_snapshot_output}"
  exit 1
fi

if ! printf '%s\n' "${fly_snapshot_output}" | node deployment/fly/validate-snapshot.mjs; then
  print_bounded_rpc_output "Fly diagnostic RPC output" "${fly_snapshot_output}"
  exit 1
fi

if ! replay_output="$(docker exec "${fly_container}" sh -lc \
  'RELEASE_NODE="paseo_relay@$FLY_PRIVATE_IP" RELEASE_DISTRIBUTION=name ERL_AFLAGS="-proto_dist inet6_tcp" ELIXIR_ERL_OPTIONS="+fnu" /app/bin/paseo_relay rpc '\''Code.require_file("/app/diagnostics/replay-e2e.exs"); PaseoRelay.FlyReplayE2E.run(["--endpoint", "ws://127.0.0.1:4000", "--owner", "ci-machine", "--landing", "ci-machine"])'\''' 2>&1)"; then
  print_bounded_rpc_output "Fly replay RPC failed" "${replay_output}"
  exit 1
fi

if ! grep --quiet '"status":"ok"' <<<"${replay_output}"; then
  print_bounded_rpc_output "Fly replay RPC output" "${replay_output}"
  exit 1
fi

# Both deployment adapters remain merge gates. The mesh probe uses the generic image.
MESH_RELAY_IMAGE="${generic_image}" deployment/mesh/e2e/run.sh --skip-build
MESH_RELAY_IMAGE="${generic_image}" deployment/mesh/e2e/binding.sh
python3 deployment/mesh/test_render.py
bash -n deployment/mesh/{render,bootstrap,deploy,rpc}.sh deployment/mesh/e2e/{run,binding}.sh
docker build --file deployment/mesh/Dockerfile.caddy --tag "paseo-relay-ci-caddy:${run_id}" .

mesh_caddy_config="$(mktemp)"
python3 - "${mesh_caddy_config}" <<'PYTHON'
from pathlib import Path
import sys
config = Path("deployment/mesh/Caddyfile.tmpl").read_text()
config = config.replace("@SITES@", "relay-ci.example.com, relay-second-ci.example.com").replace("@LOCAL@", "127.0.0.1:4000")
config = config.replace("@TLS@", "tls {\n dns cloudflare {env.CF_API_TOKEN}\n}")
Path(sys.argv[1]).write_text(config)
PYTHON
# Provision validation only: no listener, DNS request, or certificate order.
docker run --rm --env CF_API_TOKEN=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --volume "${mesh_caddy_config}:/etc/caddy/Caddyfile:ro" \
  "paseo-relay-ci-caddy:${run_id}" caddy adapt --config /etc/caddy/Caddyfile --validate >/dev/null
