"""Turns bench/results/raw/ into bench/results/README.md and the charts beside it.

Run through bench/report.sh, which provides matplotlib in a container.
"""

import json
import pathlib
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results")
RAW = ROOT / "raw"

plt.rcParams.update({"figure.dpi": 120, "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False})
C3, C5 = "#3b6ea8", "#e08a2c"


def jsonl(name):
    path = RAW / name
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def fmt(n):
    return f"{n:,.0f}"


def ms(us):
    return f"{us / 1000:.2f}"


out = []
env = (RAW / "environment.txt").read_text().strip().splitlines() if (RAW / "environment.txt").exists() else []
envd = dict(line.split(": ", 1) for line in env if ": " in line)

out += ["# Benchmark results", "",
        "Every number here is produced by `bench/run.sh` and rendered by `bench/report.sh`;",
        "nothing is typed in by hand. Raw results are in [`raw/`](raw/).", "",
        "## Environment", "",
        "| | |", "|---|---|"]
for k in ["date", "commit", "host_cpu", "host_cores", "host_memory_gb", "docker_cpus",
          "docker_memory_gb", "docker_os", "docker_kernel"]:
    if k in envd:
        out.append(f"| {k.replace('_', ' ')} | {envd[k].strip()} |")
out += ["",
        "All nodes and the load generator share one laptop through Docker Desktop's Linux VM,",
        "so absolute numbers are a lower bound for real hardware with one node per machine;",
        "the comparisons between configurations are the meaningful part. `fsync` on Docker",
        "Desktop's virtual disk is also much cheaper than on a datacenter SSD with power-loss",
        "protection turned off, and much more expensive than one with it on.", ""]

# ---- storage engine ------------------------------------------------------------------
storage = {r["bench"]: r for r in jsonl("storage.jsonl")}
if storage:
    out += ["## Storage engine alone (no Raft, no network)", "",
            "16 threads, 1 KB values, in-process.", "",
            "| Benchmark | ops/s | fsyncs | p50 | p99 | p99.9 |", "|---|---:|---:|---:|---:|---:|"]
    names = {"write_fsync_per_write": "Writes, fsync per write",
             "write_group_commit": "Writes, group commit",
             "write_no_sync": "Writes, no fsync",
             "read_random": "Random reads"}
    for k, label in names.items():
        if k in storage:
            r = storage[k]
            out.append(f"| {label} | {fmt(r['ops_per_sec'])} | {fmt(r['fsyncs'])} | "
                       f"{ms(r['p50_us'])} ms | {ms(r['p99_us'])} ms | {ms(r['p999_us'])} ms |")
    if "write_fsync_per_write" in storage and "write_group_commit" in storage:
        a, b = storage["write_fsync_per_write"], storage["write_group_commit"]
        out += ["", f"**Group commit: {b['ops_per_sec'] / a['ops_per_sec']:.1f}x the write "
                f"throughput of an fsync per write**, at "
                f"{b['ops'] / max(b['fsyncs'], 1):.1f} writes per fsync."]
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.bar(["fsync per write", "group commit"], [a["ops_per_sec"], b["ops_per_sec"]],
               color=[C5, C3])
        ax.set_ylabel("writes / s")
        ax.set_title("Durable writes, 16 concurrent writers")
        fig.tight_layout()
        fig.savefig(ROOT / "group_commit.png")
        out += ["", "![group commit](group_commit.png)"]
    out.append("")

# ---- cluster throughput and latency ------------------------------------------------------
sat = jsonl("saturation.jsonl")
lat = jsonl("latency.jsonl")
desc = {"A": "50% read, 50% update", "B": "95% read, 5% update", "C": "100% read",
        "W": "100% write"}
if sat:
    out += ["## Cluster throughput", "",
            "YCSB-style workloads, 1 KB values, Zipfian keys (theta 0.99). Saturation:",
            "8 connections with 32 requests in flight each.", "",
            "| Workload | Mix | 3 nodes ops/s | 5 nodes ops/s |", "|---|---|---:|---:|"]
    by = {(r["nodes"], r["workload"]): r for r in sat}
    for w in "ABCW":
        r3, r5 = by.get((3, w)), by.get((5, w))
        out.append(f"| {w} | {desc[w]} | {fmt(r3['throughput']) if r3 else '-'} | "
                   f"{fmt(r5['throughput']) if r5 else '-'} |")
    fig, ax = plt.subplots(figsize=(7, 3.2))
    xs = range(4)
    ax.bar([x - 0.2 for x in xs], [by.get((3, w), {}).get("throughput", 0) for w in "ABCW"],
           0.4, label="3 nodes", color=C3)
    ax.bar([x + 0.2 for x in xs], [by.get((5, w), {}).get("throughput", 0) for w in "ABCW"],
           0.4, label="5 nodes", color=C5)
    ax.set_xticks(list(xs), [f"{w}\n{desc[w]}" for w in "ABCW"], fontsize=8)
    ax.set_ylabel("ops / s")
    ax.set_title("Saturation throughput")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(ROOT / "throughput.png")
    out += ["", "![throughput](throughput.png)", ""]

if lat:
    out += ["## Latency", "",
            "Open-loop at half the saturation throughput, measured from each request's",
            "scheduled send time (no coordinated omission).", "",
            "| Nodes | Workload | Offered ops/s | p50 | p99 | p99.9 |", "|---:|---|---:|---:|---:|---:|"]
    for r in sorted(lat, key=lambda r: (r["nodes"], r["workload"])):
        out.append(f"| {r['nodes']} | {r['workload']} | {fmt(r['target_rate'])} | "
                   f"{ms(r['p50_us'])} ms | {ms(r['p99_us'])} ms | {ms(r['p999_us'])} ms |")
    fig, ax = plt.subplots(figsize=(7, 3.2))
    rows = [r for r in sorted(lat, key=lambda r: r["workload"]) if r["nodes"] == 3]
    for i, (key, label) in enumerate([("p50_us", "p50"), ("p99_us", "p99"), ("p999_us", "p99.9")]):
        ax.bar([x + (i - 1) * 0.27 for x in range(len(rows))], [r[key] / 1000 for r in rows], 0.27,
               label=label, color=[C3, C5, "#7a7a7a"][i])
    ax.set_xticks(range(len(rows)), [r["workload"] for r in rows])
    ax.set_ylabel("ms")
    ax.set_title("Latency percentiles, 3 nodes, at half saturation")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(ROOT / "latency.png")
    out += ["", "![latency](latency.png)", ""]

reads = {r["label"]: r for r in jsonl("reads.jsonl")}
if "reads-readindex" in reads and "reads-log" in reads:
    a, b = reads["reads-log"], reads["reads-readindex"]
    out += ["## Reads: ReadIndex vs. through the log", "",
            "Workload C (all reads) on 3 nodes. Both are linearizable.", "",
            "| Read path | ops/s | p50 | p99 |", "|---|---:|---:|---:|",
            f"| Log entry per read | {fmt(a['throughput'])} | {ms(a['p50_us'])} ms | {ms(a['p99_us'])} ms |",
            f"| ReadIndex | {fmt(b['throughput'])} | {ms(b['p50_us'])} ms | {ms(b['p99_us'])} ms |",
            "", f"**ReadIndex: {b['throughput'] / a['throughput']:.1f}x the read throughput.**", ""]

fail = jsonl("failover.jsonl")
if fail:
    gaps = sorted(r["max_gap_ms"] for r in fail)
    p50 = statistics.median(gaps)
    p90 = gaps[int(0.9 * (len(gaps) - 1))]
    out += ["## Failover", "",
            f"Leader killed with SIGKILL under a steady write stream, {len(gaps)} times. Failover",
            "time is the longest gap between successful writes: leader death, election timeout",
            "(150-300 ms), election, and the client finding the new leader.", "",
            "| Runs | p50 | p90 | max |", "|---:|---:|---:|---:|",
            f"| {len(gaps)} | {p50:.0f} ms | {p90:.0f} ms | {gaps[-1]:.0f} ms |"]
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.hist(gaps, bins=20, color=C3)
    ax.set_xlabel("ms from leader death to the next successful write")
    ax.set_ylabel("runs")
    ax.set_title(f"Failover time, {len(gaps)} leader kills")
    fig.tight_layout()
    fig.savefig(ROOT / "failover.png")
    out += ["", "![failover](failover.png)", ""]

rec = jsonl("recovery.jsonl")
if rec:
    times = sorted(r["ready_ms"] for r in rec)
    out += ["## Recovery", "",
            f"A single node holding {rec[0].get('data_mb', '?')} MB on disk, killed with SIGKILL and",
            "restarted, until it serves a read. Includes starting the container and the node's",
            "first election timeout.", "",
            "| Runs | median | max |", "|---:|---:|---:|",
            f"| {len(times)} | {statistics.median(times):.0f} ms | {times[-1]:.0f} ms |", ""]

load = jsonl("load.jsonl")
if load:
    out += ["## Bulk load", "", "| Nodes | Keys | Seconds | writes/s |", "|---:|---:|---:|---:|"]
    for r in load:
        out.append(f"| {r['nodes']} | {fmt(r['completed'])} | {r['seconds']:.0f} | {fmt(r['throughput'])} |")
    out.append("")

chaos = RAW / "chaos.csv"
if chaos.exists():
    rows = [l.split(",") for l in chaos.read_text().splitlines() if l.strip()]
    scen = {}
    for r in rows:
        s = scen.setdefault(r[0], {"runs": 0, "ok": 0, "ops": 0})
        s["runs"] += 1
        s["ok"] += r[2] == "linearizable"
        for kv in r[3].split():
            if kv.startswith("ops="):
                s["ops"] += int(kv[4:])
    total = sum(s["runs"] for s in scen.values())
    ok = sum(s["ok"] for s in scen.values())
    ops = sum(s["ops"] for s in scen.values())
    out += ["## Correctness under faults", "",
            f"**{ok} of {total} fault-injection runs linearizable ({total - ok} violations), "
            f"{fmt(ops)} operations checked with Porcupine.** See `chaos/`.", "",
            "| Scenario | Runs | Linearizable | Operations |", "|---|---:|---:|---:|"]
    for name, s in scen.items():
        out.append(f"| {name} | {s['runs']} | {s['ok']} | {fmt(s['ops'])} |")
    out.append("")

(ROOT / "README.md").write_text("\n".join(out) + "\n")
print(f"wrote {ROOT / 'README.md'}")
