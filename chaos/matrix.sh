#!/usr/bin/env bash
# Run every fault-injection scenario for a range of seeds and summarize.
#
#   chaos/matrix.sh [first_seed] [last_seed]     (default 1..20, the SPEC's 20 runs each)
#
# Results accumulate in out/results.csv; the summary is printed and written to
# out/summary.md.
set -uo pipefail
FIRST="${1:-1}"
LAST="${2:-20}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS="leader-crash follower-restart minority-partition leader-minority flapping-partition slow-network packet-loss data-wipe clock-skew"

mkdir -p "$HERE/out"
for seed in $(seq "$FIRST" "$LAST"); do
  for s in $SCENARIOS; do
    "$HERE/run.sh" "$s" "$seed" >/dev/null 2>&1
    echo "$(tail -n 1 "$HERE/out/results.csv")"
  done
done

{
  echo "| Scenario | Runs | Linearizable | Violations | Inconclusive | Operations checked |"
  echo "|---|---:|---:|---:|---:|---:|"
  for s in $SCENARIOS; do
    awk -F, -v s="$s" -v first="$FIRST" -v last="$LAST" '
      $1 == s && $2 >= first && $2 <= last {
        runs++
        if ($3 == "linearizable") ok++; else if ($3 == "VIOLATION") bad++; else inc++
        split($4, kv, " "); for (i in kv) if (kv[i] ~ /^ops=/) { sub(/ops=/, "", kv[i]); ops += kv[i] }
      }
      END { printf "| %s | %d | %d | %d | %d | %d |\n", s, runs, ok, bad, inc, ops }' \
      "$HERE/out/results.csv"
  done
} | tee "$HERE/out/summary.md"
