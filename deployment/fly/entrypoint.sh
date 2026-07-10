#!/bin/sh
set -eu

# This adapter is the only place provider inputs are named. The release itself
# receives the same generic contract used by every other deployment target.
if [ -n "${FLY_APP_NAME:-}" ] && [ -z "${RELEASE_NODE:-}" ]; then
  export RELEASE_NODE="paseo_relay@${FLY_MACHINE_ID}.vm.${FLY_APP_NAME}.internal"
fi

export RELEASE_DISTRIBUTION=name
export PASEO_RELAY_OWNERSHIP_TARGET="instance=${FLY_MACHINE_ID}"
export PASEO_RELAY_REROUTE_HEADER=fly-replay
export PASEO_RELAY_MIN_CLUSTER_SIZE=2

exec /app/bin/paseo_relay start
