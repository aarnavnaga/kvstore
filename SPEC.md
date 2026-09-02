# Raft KV Store: Specification

A 3 to 5 node, Raft-replicated key-value store in C++20 that survives node crashes
and network partitions without losing or reordering writes.

## What the finished system does

- Clients call `PUT`, `GET` and `DELETE` over a custom binary TCP protocol, through a CLI
  or a Java SDK.
- Every write is replicated by Raft to a majority before it is acknowledged; reads are
  linearizable.
- Each node persists data in its own LSM-tree storage engine and recovers cleanly after
  `kill -9`.
- The cluster runs on Kubernetes with mutual TLS between nodes and Prometheus metrics.

## Goals

1. **Correctness first:** zero linearizability violations across every fault-injection
   scenario.
2. **Real performance numbers:** ops/sec and p50/p99 latency, measured by a reproducible
   benchmark script.
3. **Readable engineering:** a `DESIGN.md` that explains each decision and the bugs that
   changed it.

## Non-goals

- Multi-Raft sharding, transactions, or SQL. Single Raft group, single keyspace.
- Byzantine fault tolerance. Nodes may crash or be cut off, but never lie.
- Online membership changes and backup/restore tooling. Cluster size is fixed at deploy
  time.

## Architecture

Each node is four layers stacked behind one TCP port, and a write is acknowledged only
after a majority of nodes have it in their Raft log. A client sends a write to any node;
followers redirect it to the leader, the leader replicates it to its peers, and each node
applies committed entries to its own LSM tree.

## Tech stack

| Area | Choice | Why |
|---|---|---|
| Server | C++20 (Clang 17 or GCC 13), CMake + Ninja, vcpkg | Predictable latency and direct control over memory, threads and syscalls |
| Wire format | Hand-written length-prefixed binary frames | No codegen dependency and full control over framing; Protocol Buffers is an acceptable swap |
| TLS | OpenSSL 3 | Standard for mTLS in C++ |
| Metrics | prometheus-cpp | Native Prometheus exposition |
| Tests | GoogleTest, libFuzzer, sanitizers, Porcupine (Go, checker only) | Covers unit, fuzz and linearizability layers |
| Client | Java 21, Maven, JUnit 5 | First-class client for JVM services, the most likely callers |
| Infra | Docker, Docker Compose, kind for local Kubernetes, Helm | Same manifests run locally and in the cloud |
| Bench | Custom C++ load generator, HdrHistogram, perf | Accurate tail latency and profiling |

## Repo layout

```
kvstore/
  src/net/        epoll loop, framing, TLS
  src/raft/       election, replication, snapshots
  src/storage/    memtable, WAL, SSTable, compaction, manifest
  src/server/     wiring, config, metrics
  tests/          unit, model-based, crash, raft simulation
  fuzz/           libFuzzer targets
  chaos/          compose files, partition and kill scripts, Porcupine checker
  bench/          load generator, workloads, results/
  client-java/    SDK + integration tests
  deploy/helm/    chart, values, mTLS certs job
  DESIGN.md       decisions, alternatives, bug log
```

## Milestones

Ten milestones, each ending in a merged PR with passing CI. Do not start a milestone until
the previous one's acceptance test passes.

| # | Build | Done when |
|---|---|---|
| M0 Setup | CMake + C++20, clang-format, clang-tidy, GoogleTest, Docker dev image, GitHub Actions with ASan and TSan build jobs | CI is green on a placeholder test in all three build modes |
| M1 Storage engine | Skiplist memtable; WAL with a CRC32 per record and fsync per batch; flush to SSTables (sorted data blocks, index block, bloom filter); tombstones for deletes; reads merge memtable and SSTables newest-first | A randomized test matches `std::map` over 1M ops, and `kill -9` mid-write recovers every acknowledged write |
| M2 Compaction | Background compaction thread (start with size-tiered); a MANIFEST file that swaps SSTable sets atomically | Disk usage drops after mass deletes; reads stay correct while compaction runs |
| M3 Networking | Non-blocking TCP server on epoll; length-prefixed binary frames with request IDs; worker thread pool; a CLI client | 1,000 concurrent clients with no fd leaks; libFuzzer runs the frame parser 1 hour without a crash |
| M4 Leader election | Raft terms, RequestVote, randomized election timeouts, `currentTerm` and `votedFor` persisted before replying | Exactly one leader per term across 1,000 randomized test runs; a new leader is elected after the old one is killed |
| M5 Log replication | AppendEntries, commit index, apply loop into the LSM tree; followers redirect clients to the leader | Writes survive the loss of any minority of nodes, and every node applies the same log in the same order |
| M6 Snapshots + reads | Log compaction via snapshots, InstallSnapshot for lagging nodes; linearizable reads via ReadIndex | A node wiped of its data rejoins and catches up from a snapshot |
| M7 Fault injection | Docker Compose cluster; tc netem delays, iptables partitions, process kills; client histories recorded and checked with Porcupine | Zero linearizability violations across every scenario in the test matrix |
| M8 Deploy + secure | Kubernetes StatefulSet, headless Service, Helm chart, readiness probes; mutual TLS between nodes; Prometheus metrics + Grafana dashboard; Trivy image scan | `helm install` brings up a healthy 3-node cluster with mTLS on; Trivy reports no critical CVEs |
| M9 Java SDK + polish | Java client with connection pooling, leader discovery, retries with backoff; benchmark harness; README and DESIGN.md | The SDK passes an integration test against a live cluster; benchmark charts regenerate from one command |

## Testing and correctness

The store is only as trustworthy as its proof that it never loses or reorders an
acknowledged write, so testing is built alongside each milestone, not at the end.

1. **Unit tests (GoogleTest):** memtable, WAL encode/decode, SSTable reader/writer, bloom
   filter false-positive rate, Raft state transitions.
2. **Model-based tests:** run random op sequences against the store and a `std::map`
   reference; any divergence fails with the seed printed.
3. **Crash tests:** a harness writes in a loop, sends `kill -9` at random points,
   restarts, and checks every acknowledged write is readable.
4. **Sanitizers:** every CI run builds with AddressSanitizer and ThreadSanitizer; a TSan
   report fails the build.
5. **Fuzzing (libFuzzer):** the wire-protocol parser, the WAL record decoder, and the
   SSTable block reader.
6. **Deterministic Raft simulation:** run Raft nodes in one process with a fake clock and a
   fake network that drops, delays, duplicates and reorders messages under a seed.
7. **Linearizability checks:** in the real Docker cluster, clients record each operation's
   invoke and return times; Porcupine checks the history.

### Fault-injection matrix (M7)

Each scenario runs at least 20 times with different seeds. The target is zero violations
across all runs.

| Scenario | How it is injected |
|---|---|
| Leader crash | `docker kill` the leader mid-write |
| Follower crash and restart | Kill, wait 10 s, restart with data intact |
| Minority partition | iptables isolates 1 of 3 nodes |
| Leader on the minority side | Partition the leader away from both followers |
| Flapping partition | Heal and re-split every 2 s for 60 s |
| Slow network | tc netem adds 100 to 500 ms latency with jitter |
| Packet loss | tc netem drops 10% of packets |
| Data wipe | Delete one node's data dir, restart, catch up from snapshot |
| Clock skew | Run one node under libfaketime at 2x clock speed; elections must still converge |

## Benchmarks and metrics

Every number comes from a script in `bench/` that anyone can rerun, with the machine specs
written next to the results.

Workloads (YCSB-style, 1 KB values, 1M keys, Zipfian key distribution):

| Workload | Mix | What it shows |
|---|---|---|
| A | 50% reads, 50% updates | Balanced cost of replication and reads |
| B | 95% reads, 5% updates | Read path and ReadIndex overhead |
| C | 100% reads | Upper bound for read throughput |
| W | 100% writes | WAL, fsync batching, and replication cost |

Metrics to report:

- Throughput (ops/sec) per workload on 3 and 5 nodes.
- Latency p50, p99 and p99.9 per workload.
- Effect of group commit: write throughput with fsync per write vs. batched fsync.
- Failover time: leader killed to first successful write on the new leader, p50 over 50 runs.
- Recovery time: restart to serving after 1 GB of data.
- Storage engine alone (single node, no Raft): writes/sec and reads/sec.
- Correctness: scenarios and runs passed, violations found (target 0).

Tooling: an open-loop C++ load generator (fixed request rate, so coordinated omission does
not hide latency), HdrHistogram for percentiles, and `perf` flame graphs.

## Risks

| Risk | Fallback |
|---|---|
| Subtle Raft bugs | Build the deterministic simulator in M4, before real networking touches Raft |
| C++ memory and threading bugs | Sanitizers on every CI run from M0; keep shared state behind one lock until profiling says otherwise |
| Compaction complexity | Ship size-tiered compaction only; leveled compaction is a stretch goal |
| Kubernetes and mTLS hide cluster bugs | Do M7 entirely in Docker Compose first; Kubernetes only repackages a working cluster |

## Resources

- Raft paper (Ongaro and Ousterhout, extended version), especially Figure 2.
- MIT 6.5840 Distributed Systems lectures and labs.
- LevelDB implementation notes for the SSTable, log and compaction design.
- Porcupine, the linearizability checker used in M7.
- YCSB for the benchmark workload definitions.
- *Designing Data-Intensive Applications* (Kleppmann), chapters 3, 5 and 9.
