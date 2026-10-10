#!/bin/sh
# Container entrypoint for kvserver.
#
#   KV_ID, KV_PEERS, KV_DATA, KV_LISTEN, KV_TICK_MS  map to kvserver flags.
#   KV_EXTRA_ARGS           appended to the kvserver command line.
#   $KV_DATA/JOIN           marker left by an operator (or the chaos harness) after
#                           wiping the disk: start with --join, once. The node persists
#                           its joining state itself from then on.
#   KV_CLOCK_RATE=2         run under libfaketime with the clock at 2x speed.
#   KV_ID_FROM_HOSTNAME=1   derive KV_ID from a StatefulSet pod name: kvstore-0 is node 1.
#   KV_TLS_DIR              directory with ca.crt, tls.crt and tls.key: enables mutual TLS.
set -eu

if [ "${1:-}" != "kvserver" ]; then
  exec "$@"
fi
shift

if [ -z "${KV_ID:-}" ] && [ "${KV_ID_FROM_HOSTNAME:-}" = "1" ]; then
  ORDINAL="${HOSTNAME##*-}"
  KV_ID=$((ORDINAL + 1))
fi

TLS=""
if [ -n "${KV_TLS_DIR:-}" ]; then
  TLS="--tls_ca=$KV_TLS_DIR/ca.crt --tls_cert=$KV_TLS_DIR/tls.crt --tls_key=$KV_TLS_DIR/tls.key"
fi

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
  --admin_listen="${KV_ADMIN_LISTEN:-0.0.0.0:9100}" \
  $TLS $EXTRA ${KV_EXTRA_ARGS:-} "$@"
