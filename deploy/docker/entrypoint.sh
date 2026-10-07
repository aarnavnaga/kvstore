#!/bin/sh
# Container entrypoint for kvserver.
#
#   KV_ID, KV_PEERS, KV_DATA, KV_LISTEN, KV_TICK_MS  map to kvserver flags.
#   KV_EXTRA_ARGS           appended to the kvserver command line.
#   $KV_DATA/JOIN           marker left by an operator (or the chaos harness) after
#                           wiping the disk: start with --join, once. The node persists
#                           its joining state itself from then on.
#   KV_CLOCK_RATE=2         run under libfaketime with the clock at 2x speed.
set -eu

if [ "${1:-}" != "kvserver" ]; then
  exec "$@"
fi
shift

DATA="${KV_DATA:-/var/lib/kvstore}"
mkdir -p "$DATA"
EXTRA=""
if [ -f "$DATA/JOIN" ]; then
  rm -f "$DATA/JOIN"
  EXTRA="--join"
fi

if [ -n "${KV_CLOCK_RATE:-}" ]; then
  LIBFT=$(find /usr/lib -name 'libfaketime.so.1' | head -n 1)
  export LD_PRELOAD="$LIBFT"
  export FAKETIME="+0 x${KV_CLOCK_RATE}"
  export FAKETIME_NO_CACHE=1
  # libfaketime 0.9.10 leaves CLOCK_MONOTONIC alone by default, and steady_clock (which
  # drives Raft's timers) reads it; without this the "skewed" node runs at normal speed.
  export FAKETIME_DONT_FAKE_MONOTONIC=0
fi

exec kvserver \
  --id="${KV_ID:?KV_ID is required}" \
  --peers="${KV_PEERS:?KV_PEERS is required}" \
  --listen="${KV_LISTEN:-0.0.0.0:7000}" \
  --data="$DATA" \
  --tick_ms="${KV_TICK_MS:-10}" \
  $EXTRA ${KV_EXTRA_ARGS:-} "$@"
