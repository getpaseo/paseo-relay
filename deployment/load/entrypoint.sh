#!/bin/sh
set -eu

ulimit -n "${PASEO_RELAY_LOAD_NOFILE:-30000}"
exec "$@"
