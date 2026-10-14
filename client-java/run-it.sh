#!/usr/bin/env bash
# Starts a 3-node cluster in Docker and runs the Java SDK's tests against it, including
# a leader failover in the middle of a write stream.
#
#   client-java/run-it.sh        (uses the kvstore:dev image; `make image` first)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT=kvjava
COMPOSE=(docker compose -p "$PROJECT" -f "$ROOT/deploy/compose/cluster3.yml")
FAULTS="$(mktemp -d)"
cleanup() {
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  rm -rf "$FAULTS"
}
trap cleanup EXIT

"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d >/dev/null 2>&1
SERVERS=node1:7000,node2:7000,node3:7000
leader() {
  docker exec "$PROJECT-node1-1" kvcli --servers=$SERVERS --timeout_ms=1000 status 2>/dev/null \
    | awk '/role=leader/ { sub(/:.*/, "", $1); sub(/^node/, "", $1); print $1; exit }'
}
for _ in $(seq 1 50); do [ -n "$(leader)" ] && break; sleep 0.2; done
echo "cluster up, leader node$(leader)"

# The failover test asks for a leader kill by creating kill-leader; we answer with
# leader-restarted once the old leader is back.
(
  while [ ! -f "$FAULTS/kill-leader" ]; do sleep 0.2; done
  L=$(leader)
  echo "harness: kill -9 node$L"
  docker kill -s KILL "$PROJECT-node$L-1" >/dev/null
  sleep 4
  echo "harness: restart node$L"
  docker start "$PROJECT-node$L-1" >/dev/null
  touch "$FAULTS/leader-restarted"
) &

docker run --rm --network "${PROJECT}_default" \
  -v "$ROOT/client-java:/src" -v kvstore-m2:/root/.m2 -v "$FAULTS:/faults" -w /src \
  -e KVSTORE_SERVERS="$SERVERS" -e KVSTORE_FAULT_DIR=/faults \
  maven:3.9.9-eclipse-temurin-21 mvn -B test
