#!/usr/bin/env bash
# Run one fault-injection scenario against a fresh 3-node cluster and check the client
# history for linearizability.
#
#   chaos/run.sh <scenario> [seed]
#
# Scenarios: leader-crash follower-restart minority-partition leader-minority
#            flapping-partition slow-network packet-loss data-wipe clock-skew
#
# Writes out/<scenario>-<seed>.jsonl (history), .log (harness + checker output),
# .html (Porcupine's visualization, only on a violation), and appends one line to
# out/results.csv. Exit status 0 means the history was linearizable.
set -euo pipefail

SCENARIO="${1:?usage: chaos/run.sh <scenario> [seed]}"
SEED="${2:-1}"
DURATION="${DURATION:-20}"  # seconds of fault injection
SETTLE="${SETTLE:-6}"       # seconds of healed operation before the history ends
CLIENTS="${CLIENTS:-8}"
KEYS="${KEYS:-10}"
[ "$SCENARIO" = "flapping-partition" ] && DURATION="${FLAP_DURATION:-60}"

HERE="$(cd "$(dirname "$0")" && pwd)"
export KV_IMAGE="${KV_IMAGE:-kvstore:dev}"  # the image under test
cd "$HERE"
mkdir -p out bin
NAME="$SCENARIO-$SEED"
LOG="out/$NAME.log"
: > "$LOG"
RANDOM=$SEED  # every random choice below replays from the seed

COMPOSE=(docker compose -p kvchaos -f docker-compose.yml)
[ "$SCENARIO" = "clock-skew" ] && COMPOSE+=(-f skew.override.yml)
if [ "${UNSAFE:-0}" = "1" ]; then
  COMPOSE+=(-f unsafe.override.yml)
  NAME="$NAME-unsafe"
  LOG="out/$NAME.log"
  : > "$LOG"
fi

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG" >&2; }
ctr() { echo "kvchaos-node$1-1"; }
ip_of() { echo "172.28.0.1$1"; }
nx() { docker exec "$(ctr "$1")" "${@:2}"; }

# leader prints the node number a majority calls leader, or nothing.
leader() {
  local out
  out=$(docker exec "$(ctr 1)" kvcli --servers=node1:7000,node2:7000,node3:7000 \
        --timeout_ms=1000 status 2>/dev/null || true)
  echo "$out" | awk '/role=leader/ { sub(/^node/, "", $1); sub(/:.*/, "", $1); print $1; exit }'
}

wait_leader() {
  for _ in $(seq 1 100); do
    local l
    l=$(leader)
    if [ -n "$l" ]; then echo "$l"; return 0; fi
    sleep 0.2
  done
  return 1
}

# A follower: any node that is not the current leader.
follower() {
  local l=$1
  local f=$(( (RANDOM % 2) + 1 ))
  [ "$f" -ge "$l" ] && f=$((f + 1))
  echo "$f"
}

kill_node() { log "kill -9 node$1"; docker kill -s KILL "$(ctr "$1")" >/dev/null; }
start_node() { log "start node$1"; docker start "$(ctr "$1")" >/dev/null; }

cut() {  # cut <a> <b>: drop all traffic between nodes a and b
  nx "$1" iptables -A INPUT -s "$(ip_of "$2")" -j DROP
  nx "$1" iptables -A OUTPUT -d "$(ip_of "$2")" -j DROP
}

isolate() {
  log "isolate node$1"
  for o in 1 2 3; do [ "$o" != "$1" ] && cut "$1" "$o"; done
  return 0
}

heal() {
  for n in 1 2 3; do
    nx "$n" iptables -F 2>/dev/null || true
    nx "$n" tc qdisc del dev eth0 root 2>/dev/null || true
  done
}

netem() {  # netem <args...> on every node's egress
  log "netem $*"
  for n in 1 2 3; do nx "$n" tc qdisc add dev eth0 root netem "$@"; done
}

wipe_node() {
  local n=$1
  log "wipe node$n: stop, delete data, restart with --join"
  docker stop -t 0 "$(ctr "$n")" >/dev/null
  docker run --rm -v "kvchaos_node$n:/data" --entrypoint sh "$KV_IMAGE" \
    -c 'rm -rf /data/* && touch /data/JOIN'
  docker start "$(ctr "$n")" >/dev/null
}

cleanup() {
  docker rm -f kvchaos-recorder >/dev/null 2>&1 || true
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- bring up a fresh cluster ------------------------------------------------------------
cleanup
"${COMPOSE[@]}" up -d node1 node2 node3 >/dev/null 2>&1
L=$(wait_leader) || { log "no leader at startup"; exit 2; }
log "scenario=$SCENARIO seed=$SEED leader=node$L duration=${DURATION}s"

docker run -d --name kvchaos-recorder --network kvchaos_kv -v "$HERE/out:/out" \
  --entrypoint kvhistory "$KV_IMAGE" \
  --servers=node1:7000,node2:7000,node3:7000 --clients="$CLIENTS" --keys="$KEYS" \
  --duration_s=$((DURATION + SETTLE)) --seed="$SEED" --out="/out/$NAME.jsonl" --read_any >/dev/null
sleep 1

# ---- inject faults -------------------------------------------------------------------------
T0=$(date +%s)
remaining() { echo $(( DURATION - ($(date +%s) - T0) )); }

case "$SCENARIO" in
  leader-crash)
    # Kill whoever leads, mid-write, twice.
    for _ in 1 2; do
      sleep $(( 2 + RANDOM % 3 ))
      L=$(wait_leader) || true
      [ -n "$L" ] && kill_node "$L"
      sleep $(( 3 + RANDOM % 3 ))
      [ -n "$L" ] && start_node "$L"
    done
    ;;
  follower-restart)
    sleep $(( 2 + RANDOM % 3 ))
    F=$(follower "$(wait_leader)")
    kill_node "$F"
    sleep 10
    start_node "$F"
    ;;
  minority-partition)
    sleep $(( 2 + RANDOM % 3 ))
    isolate "$(follower "$(wait_leader)")"
    sleep 12
    log "heal"; heal
    ;;
  leader-minority)
    sleep $(( 2 + RANDOM % 3 ))
    isolate "$(wait_leader)"
    sleep 12
    log "heal"; heal
    ;;
  flapping-partition)
    # Heal and re-split every 2 seconds, isolating a random node each time.
    while [ "$(remaining)" -gt 0 ]; do
      heal
      isolate $(( 1 + RANDOM % 3 ))
      sleep 2
    done
    log "heal"; heal
    ;;
  slow-network)
    sleep 2
    netem delay 300ms 200ms  # 100-500 ms per hop
    sleep $(( DURATION - 4 ))
    log "heal"; heal
    ;;
  packet-loss)
    sleep 2
    netem loss 10%
    sleep $(( DURATION - 4 ))
    log "heal"; heal
    ;;
  data-wipe)
    sleep $(( 4 + RANDOM % 3 ))  # let the leader compact past the beginning first
    wipe_node "$(follower "$(wait_leader)")"
    ;;
  clock-skew)
    # node3 runs at 2x clock speed throughout (skew.override.yml). Force elections by
    # cutting off the leader for a few seconds (not killing it, so tick counters
    # survive for the rate check below).
    for _ in 1 2; do
      sleep $(( 3 + RANDOM % 3 ))
      L=$(wait_leader) || true
      [ -n "$L" ] && isolate "$L"
      sleep 4
      log "heal"; heal
    done
    ;;
  *)
    log "unknown scenario $SCENARIO"; exit 2 ;;
esac

# ---- heal, let the history finish, check -----------------------------------------------------
left=$(remaining)
[ "$left" -gt 0 ] && sleep "$left"
heal
for n in 1 2 3; do docker start "$(ctr "$n")" >/dev/null 2>&1 || true; done
docker wait kvchaos-recorder >/dev/null
docker logs kvchaos-recorder 2>&1 | tee -a "$LOG" >&2
docker rm -f kvchaos-recorder >/dev/null

L=$(wait_leader) || true
STATUS=$(docker exec "$(ctr 1)" kvcli --servers=node1:7000,node2:7000,node3:7000 status 2>&1 || true)
echo "$STATUS" >> "$LOG"
if [ "$SCENARIO" = "clock-skew" ]; then
  # Prove the skew was real: node3 must have ticked about twice as often as node1.
  t1=$(echo "$STATUS" | awk '/^node1/ { for (i = 1; i <= NF; i++) if ($i ~ /^ticks=/) { sub(/ticks=/, "", $i); print $i } }')
  t3=$(echo "$STATUS" | awk '/^node3/ { for (i = 1; i <= NF; i++) if ($i ~ /^ticks=/) { sub(/ticks=/, "", $i); print $i } }')
  RATIO=$(awk -v a="$t3" -v b="$t1" 'BEGIN { if (b > 0) printf "%.2f", a / b; else print 0 }')
  log "clock rate check: node3 ticks=$t3 node1 ticks=$t1 ratio=$RATIO"
fi

if [ ! -x bin/checker ]; then
  docker run --rm -v "$HERE/checker:/src" -v "$HERE/bin:/out" -w /src golang:1.23 \
    go build -o /out/checker . >/dev/null
fi
set +e
CHECK=$(docker run --rm -v "$HERE:/w" --entrypoint /w/bin/checker "$KV_IMAGE" \
  -history "/w/out/$NAME.jsonl" -viz "/w/out/$NAME.html" 2>&1)
RC=$?
set -e
if [ "$SCENARIO" = "clock-skew" ] && awk -v r="${RATIO:-0}" 'BEGIN { exit !(r < 1.6) }'; then
  log "node3's clock did not run fast; the skew was not applied"
  RC=2
fi
log "checker: $CHECK"
VERDICT=$([ $RC -eq 0 ] && echo linearizable || ([ $RC -eq 1 ] && echo VIOLATION || echo inconclusive))
echo "$SCENARIO,$SEED,$VERDICT,$(echo "$CHECK" | tr ' ' '\n' | grep -E '^(ops|unknown)=' | tr '\n' ' ')" >> out/results.csv
log "verdict: $VERDICT"
exit $RC
