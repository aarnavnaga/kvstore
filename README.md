# kvstore

A Raft-replicated key-value store in C++20, on its own LSM-tree storage engine. Three to
five nodes survive crashes, partitions, packet loss, clock skew and wiped disks without
losing or reordering an acknowledged write, and every client operation is linearizable.

Every claim below is backed by a test or a script that anyone can re-run: the fault
matrix by `chaos/matrix.sh`, the numbers by `bench/run.sh`. Nothing is typed in by hand.

---

## What it is

```
            clients (kvcli, Java SDK, kvbench)
                 │   binary frames over TCP, mutual TLS
                 ▼
 ┌──────────────────────────── one node ─────────────────────────────┐
 │  epoll server ── worker pool ──► Replica (one Raft thread)         │
 │                                    │ propose / ReadIndex            │
 │                                    ▼                                │
 │                          Raft core (deterministic) ◄──► peers       │
 │                            │ append + fsync                         │
 │                            ▼                                        │
 │                          raft.log                                   │
 │                            │ committed entries                      │
 │                            ▼                                        │
 │          state machine ──► LSM tree: WAL ─ memtable ─ SSTables      │
 └─────────────────────────────────────────────────────────────────────┘
```

| Layer | What it does | Where |
|---|---|---|
| Storage | Skiplist memtable, CRC-framed WAL with group commit, SSTables with bloom filters and per-block CRCs, size-tiered compaction, atomic MANIFEST | `src/storage/` |
| Network | Non-blocking epoll server, length-prefixed frames with request ids, async responders, mutual TLS 1.3 | `src/net/` |
| Consensus | Raft as a pure state machine: elections with PreVote and CheckQuorum, log replication, snapshots, ReadIndex reads, safe rejoin after a wiped disk | `src/raft/` |
| Server | One Raft thread, batched proposals, exactly-once client sessions, Prometheus metrics, health and readiness | `src/server/` |
| Clients | C++ client and CLI; Java 21 SDK with pooling, leader discovery and retries | `src/client/`, `client-java/` |
| Deploy | Docker image, Helm chart (StatefulSet, headless service, generated CA, PDB), Prometheus and Grafana | `deploy/` |

Design decisions, the alternatives that lost, and every bug that changed the design are
in **[DESIGN.md](DESIGN.md)**.

---

## Correctness

The store is only as credible as its proof that it never loses or reorders an
acknowledged write. That proof has five layers.

| Layer | What it checks | Scale |
|---|---|---|
| Model-based | The storage engine vs. `std::map` under random operations, with constant flushes and reopens | 1M operations |
| Crash | `kill -9` at random points; every acknowledged put and delete must survive | 25 rounds |
| Deterministic simulation | Raft nodes in one process with a seeded clock and a network that drops, delays, duplicates and reorders; election safety, state-machine safety and read freshness checked after every message | 2,000 randomized runs per CI build (election, replication, snapshots) |
| Fuzzing | libFuzzer on the frame parser, WAL replay and SSTable parsing | 112.8M inputs in 1 hour, no crash |
| Linearizability | A real Docker cluster under nine fault types; client histories checked with Porcupine | **180 runs (9 faults x 20 seeds), 4.3M operations, 0 violations** |

**The tests are tested.** Five deliberately injected Raft bugs, including the paper's
Figure 8 commit bug, are each caught by the simulator, and a deliberately broken server
that serves stale reads is caught by the Porcupine pipeline. Several were only caught
after the harness was strengthened; DESIGN.md's bug log says how.

---

## Quickstart

Everything builds and runs in Docker; the only local dependency is Docker itself.

```bash
make dev-image && make test      # build and run the C++ tests (also: make asan, make tsan)
make image                       # production image

# A 3-node cluster, and the CLI against it
docker compose -p kv -f deploy/compose/cluster3.yml up -d
docker exec kv-node1-1 kvcli --servers=node1:7000,node2:7000,node3:7000 put greeting hello
docker exec kv-node1-1 kvcli --servers=node1:7000,node2:7000,node3:7000 get greeting
docker exec kv-node1-1 kvcli --servers=node1:7000,node2:7000,node3:7000 status
```

```bash
make chaos SCENARIO=leader-minority SEED=1   # one fault scenario, checked with Porcupine
make chaos-matrix                            # all nine scenarios x 20 seeds (~2 hours)
make k8s-e2e                                 # kind + Helm: TLS, failover, rolling restart
client-java/run-it.sh                        # Java SDK against a live cluster
bench/run.sh && bench/report.sh              # every benchmark number and chart
```

### Java SDK

```java
try (KvClient kv = KvClient.builder()
        .servers("node1:7000", "node2:7000", "node3:7000")
        .build()) {
  kv.put("greeting", "hello");
  Optional<String> v = kv.getString("greeting");  // linearizable
}
```

---

## Results

Measured on one Apple M3 laptop: every node and the load generator in Docker Desktop's
VM, sharing 8 cores. Treat absolute numbers as a lower bound; the within-run comparisons
are the point. YCSB-style workloads, 1 KB values, 1M keys, Zipfian.

| | 3 nodes |
|---|---|
| Throughput | **28,900 writes/s** (W), **95,500 reads/s** (C), 22,200 ops/s mixed (A) |
| Latency at half load | p99 **10.4 ms** mixed (A), **1.4 ms** reads (C), 21.7 ms writes (W) |
| Group commit (storage engine) | **5.8x** the durable write throughput of an fsync per write |
| ReadIndex vs. a log entry per read | **2.8x** read throughput; p99 3.8 ms vs. 14.7 ms |
| Failover (leader `kill -9`, 50 runs) | **p50 284 ms**, max 340 ms to the next successful write |
| Recovery with 1 GB on disk | **298 ms** median from process start to first served read |
| Correctness | **180 / 180** fault-injection runs linearizable, 4.3M operations |

Latency is measured open-loop from each request's scheduled time, so coordinated
omission cannot hide the tail.

Full tables, charts and the machine they ran on: **[bench/results/](bench/results/)**.

---

## Layout

```
src/storage/    memtable, WAL, SSTables, compaction, manifest
src/raft/       Raft core, file and memory log storage, wire codec
src/net/        epoll server, frames, client connection, TLS
src/server/     replica, state machine, peer transport, metrics, kvserver
src/client/     C++ client          src/tools/    kvcli, kvhistory
tests/          unit, model-based, crash, Raft simulator, cluster, TLS
fuzz/           libFuzzer targets
chaos/          compose cluster, fault scenarios, Porcupine checker
bench/          load generator, storage benchmark, run and report scripts
client-java/    Java SDK and integration tests
deploy/         Docker image, Compose clusters, Helm chart, kind end-to-end test
```
