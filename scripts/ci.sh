#!/usr/bin/env bash
set -euo pipefail

readonly release_port="${PASEO_RELAY_CI_RELEASE_PORT:-4400}"
readonly container_port="${PASEO_RELAY_CI_CONTAINER_PORT:-4401}"
readonly run_id="${GITHUB_RUN_ID:-local}-$$"
readonly generic_image="paseo-relay-ci-generic:${run_id}"
readonly generic_container="paseo-relay-ci-generic-${run_id}"

release_pid=""

cleanup() {
  if [[ -n "${release_pid}" ]] && kill -0 "${release_pid}" 2>/dev/null; then
    kill "${release_pid}"
    wait "${release_pid}" || true
  fi

  docker rm --force "${generic_container}" >/dev/null 2>&1 || true
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

docker run --detach --name "${generic_container}" \
  --publish "127.0.0.1:${container_port}:4000" "${generic_image}" >/dev/null
wait_for_endpoint "http://127.0.0.1:${container_port}/health"
assert_operations_contract "http://127.0.0.1:${container_port}"
docker restart "${generic_container}" >/dev/null
wait_for_endpoint "http://127.0.0.1:${container_port}/health"
assert_operations_contract "http://127.0.0.1:${container_port}"
