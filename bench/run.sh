#!/usr/bin/env bash
# Reproduces every number in bench/results/ (about 30 minutes):
#
#   bench/run.sh            then   bench/report.sh   (tables and charts from raw/)
#
# Needs the dev image and a release build (make dev-image build-release) and the
# production image (make image). Raw results land in bench/results/raw/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RAW="$ROOT/bench/results/raw"
KEYS="${KEYS:-1000000}"       # YCSB keyspace (1 KB values: ~1 GB per node)
DURATION="${DURATION:-20}"    # measured seconds per run, after a 3 s warmup
FAILOVERS="${FAILOVERS:-50}"
mkdir -p "$RAW"
rm -f "$RAW"/*.jsonl

say() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# kvbench runs from the dev image (it links HdrHistogram) on the cluster's network.
kvbench() {
  local net=$1
  shift
  docker run --rm --network "$net" -v "$ROOT:/src" kvstore-dev /src/build/release/bench/kvbench "$@"
}

leader_of() {  # leader_of <project> <nodes>
  local servers
  servers=$(seq -s, -f "node%g:7000" 1 "$2")
  docker exec "$1-node1-1" kvcli --servers="$servers" --timeout_ms=1000 status 2>/dev/null \
    | awk '/role=leader/ { sub(/:.*/, "", $1); sub(/^node/, "", $1); print $1; exit }'
}

wait_leader() {
  for _ in $(seq 1 100); do
    [ -n "$(leader_of "$1" "$2")" ] && return 0
    sleep 0.2
  done
  echo "no leader in $1" >&2
  return 1
}

# ---- environment -----------------------------------------------------------------------
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "commit: $(git -C "$ROOT" rev-parse --short HEAD)"
  echo "host_cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
  echo "host_cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
  echo "host_memory_gb: $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 ))"
  echo "docker_cpus: $(docker info --format '{{.NCPU}}')"
  echo "docker_memory_gb: $(( $(docker info --format '{{.MemTotal}}') / 1073741824 ))"
  echo "docker_kernel: $(docker info --format '{{.KernelVersion}}')"
  echo "docker_os: $(docker info --format '{{.OperatingSystem}}')"
} > "$RAW/environment.txt"

# ---- storage engine alone ---------------------------------------------------------------
say "storage engine benchmark"
docker volume create kvbench-storage >/dev/null
docker run --rm -v kvbench-storage:/data -v "$ROOT:/src" kvstore-dev \
  /src/build/release/bench/storage_bench --dir=/data/sb --threads=16 --seconds="$DURATION" \
  > "$RAW/storage.jsonl"
docker volume rm kvbench-storage >/dev/null

# ---- YCSB workloads on 3 and 5 nodes --------------------------------------------------------
for NODES in 3 5; do
  P="kvbench$NODES"
  C=(docker compose -p "$P" -f "$ROOT/deploy/compose/cluster$NODES.yml")
  NET="${P}_default"
  SERVERS=$(seq -s, -f "node%g:7000" 1 "$NODES")
  "${C[@]}" down -v >/dev/null 2>&1 || true
  "${C[@]}" up -d >/dev/null 2>&1
  wait_leader "$P" "$NODES"
  say "$NODES nodes: loading $KEYS keys"
  kvbench "$NET" --mode=load --servers="$SERVERS" --keys="$KEYS" --connections=8 --depth=64 \
    --label="load-$NODES" --nodes="$NODES" >> "$RAW/load.jsonl"
  for W in A B C W; do
    say "$NODES nodes: workload $W, saturation"
    out=$(kvbench "$NET" --mode=closed --servers="$SERVERS" --workload="$W" --keys="$KEYS" \
      --connections=8 --depth=32 --duration_s="$DURATION" --nodes="$NODES" --label="sat-$NODES-$W")
    echo "$out" >> "$RAW/saturation.jsonl"
    # Latency at half the saturation throughput: a realistic operating point, below the
    # knee where queueing dominates.
    rate=$(echo "$out" | sed -E 's/.*"throughput":([0-9.]+).*/\1/' | awk '{ printf "%d", $1 / 2 }')
    say "$NODES nodes: workload $W, latency at $rate ops/s"
    kvbench "$NET" --mode=open --servers="$SERVERS" --workload="$W" --keys="$KEYS" \
      --connections=8 --rate="$rate" --duration_s="$DURATION" --nodes="$NODES" \
      --label="lat-$NODES-$W" >> "$RAW/latency.jsonl"
  done
  "${C[@]}" down -v >/dev/null 2>&1
done

# ---- ReadIndex vs. reads through the log --------------------------------------------------
P=kvbenchreads
C=(docker compose -p "$P" -f "$ROOT/deploy/compose/cluster3.yml")
for MODE in readindex log; do
  "${C[@]}" down -v >/dev/null 2>&1 || true
  KV_EXTRA_ARGS="--reads=$MODE" "${C[@]}" up -d >/dev/null 2>&1
  wait_leader "$P" 3
  say "reads via $MODE"
  kvbench "${P}_default" --mode=load --servers=node1:7000,node2:7000,node3:7000 --keys=100000 \
    --connections=8 --depth=64 >/dev/null
  kvbench "${P}_default" --mode=closed --servers=node1:7000,node2:7000,node3:7000 --workload=C \
    --keys=100000 --connections=8 --depth=32 --duration_s="$DURATION" --nodes=3 \
    --label="reads-$MODE" >> "$RAW/reads.jsonl"
done
"${C[@]}" down -v >/dev/null 2>&1

# ---- failover time ------------------------------------------------------------------------
P=kvbenchfail
C=(docker compose -p "$P" -f "$ROOT/deploy/compose/cluster3.yml")
"${C[@]}" down -v >/dev/null 2>&1 || true
"${C[@]}" up -d >/dev/null 2>&1
wait_leader "$P" 3
say "failover: $FAILOVERS leader kills"
for i in $(seq 1 "$FAILOVERS"); do
  wait_leader "$P" 3
  L=$(leader_of "$P" 3)
  kvbench "${P}_default" --mode=probe --servers=node1:7000,node2:7000,node3:7000 --duration_s=5 \
    >> "$RAW/failover.jsonl" &
  probe=$!
  sleep 2
  docker kill -s KILL "$P-node$L-1" >/dev/null
  wait $probe
  docker start "$P-node$L-1" >/dev/null
  sleep 1
done
"${C[@]}" down -v >/dev/null 2>&1

# ---- recovery time with ~1 GB of data ----------------------------------------------------
# The node and the probe share one container, so the clock starts the moment the server
# process is spawned (a separate probe container would start its timer late).
say "recovery: loading $KEYS keys into one node, then kill -9 and restart 5 times"
docker volume rm kvbench-recover >/dev/null 2>&1 || true
docker run --rm -v kvbench-recover:/data -v "$ROOT:/src" -e KEYS="$KEYS" kvstore-dev bash -c '
  B=/src/build/release
  start() { $B/src/kvserver --id=1 --peers=1=127.0.0.1:7000 --listen=127.0.0.1:7000 \
              --admin_listen= --data=/data 2>/dev/null & P=$!; }
  start
  $B/bench/kvbench --mode=wait --servers=127.0.0.1:7000 >/dev/null
  $B/bench/kvbench --mode=load --servers=127.0.0.1:7000 --keys=$KEYS --connections=8 --depth=64 >/dev/null
  MB=$(du -sm /data | cut -f1)
  for i in 1 2 3 4 5; do
    kill -9 $P; wait $P 2>/dev/null
    start
    $B/bench/kvbench --mode=wait --servers=127.0.0.1:7000 | sed -E "s/\}\$/,\"data_mb\":$MB}/"
  done
  kill -9 $P' > "$RAW/recovery.jsonl"
docker volume rm kvbench-recover >/dev/null

# ---- correctness -----------------------------------------------------------------------------
if [ -f "$ROOT/chaos/out/results.csv" ]; then
  cp "$ROOT/chaos/out/results.csv" "$RAW/chaos.csv"
fi

say "done; run bench/report.sh to regenerate tables and charts"
