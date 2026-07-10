#!/bin/sh
set -eu

# This adapter is the only place provider inputs are named. The release itself
# receives the same generic contract used by every other deployment target.
if [ -n "${FLY_APP_NAME:-}" ] && [ -z "${RELEASE_NODE:-}" ]; then
  export RELEASE_NODE="paseo_relay@${FLY_APP_NAME}"
fi

exec /app/bin/paseo_relay start
