# Design

Decisions, the alternatives that lost, and the bugs that changed the design. Each section
is added in the milestone that made the decision.

## Build and toolchain (M0)

**Build inside a Linux container, even on a Mac.** The network layer is built on epoll,
and ThreadSanitizer, libFuzzer and `perf` are all first-class on Linux and partial on
macOS. A `Makefile` wraps every target in `docker run` against `deploy/docker/Dockerfile.dev`,
so a laptop and CI run the same compiler (Clang 18), the same standard library and the
same sanitizers.

**Dependencies come from the OS image, not vcpkg.** The spec suggests vcpkg. Every
dependency so far (GoogleTest, OpenSSL) is packaged by Ubuntu 24.04, and installing them
with `apt` keeps a cold CI build under a minute instead of compiling them from source.
CMake falls back to fetching GoogleTest if it is not installed, so a non-Ubuntu machine
still builds.

**Three build modes on every push.** `release`, `asan` (AddressSanitizer + UBSan) and
`tsan` (ThreadSanitizer) run the full test suite in CI. Any sanitizer report fails the
build (`halt_on_error=1`). ASan and TSan cannot be combined in one binary, which is why
they are separate jobs.

## Storage engine (M1)

Each node stores data in its own LSM tree (`src/storage/`). The write path is append-only
and the read path merges sorted runs, which suits a replicated log: the Raft apply loop
produces a steady stream of small writes and almost never updates in place.

```
Put/Delete ──► WAL (append + fsync) ──► memtable (skiplist)
                                             │ full
                                             ▼
                                     immutable memtable ──► background flush ──► SSTable
Get: memtable ─► immutable memtable ─► SSTables newest-first (bloom filter first)
```

**Memtable: a skiplist with lock-free reads.** One writer at a time inserts (the write
path serializes them anyway, see group commit below); readers never lock. Nodes are
allocated from an arena and never freed individually, and each node is fully built before
a `memory_order_release` store links it in, so a reader's `acquire` load can never see a
half-initialized node. A balanced tree under a reader-writer lock would make every GET
contend with every PUT on the lock's cache line.

Entries are ordered by key, then by sequence number *descending*, so the newest version of
a key is the first one a seek lands on and overwrites never modify existing nodes.

**WAL: one CRC-framed record per write batch.** A record is `crc32c | length | payload`,
written with a single `write()`. After a crash the tail record may be torn (the process
died between `write` and `fsync`, or the disk persisted only some sectors). The reader
treats the first record whose CRC fails as the end of the log. That record was never
acknowledged, because acknowledgement waits for `fsync`, so dropping it loses nothing a
client was promised. A record that passes its CRC but fails to decode is different: it is
reported as corruption and recovery stops, rather than silently skipping data.

**Group commit.** `fsync` costs 0.1 to 10 ms depending on the device, which caps a naive
store at a few hundred writes per second. Writers instead join a queue; the writer at the
front appends its own batch plus everyone queued behind it as one WAL record, issues one
`fsync`, applies them all to the memtable and wakes the others with the shared result.
Under load N concurrent writes cost about one `fsync`. The `group_commit` option exists
only so the benchmark can measure the difference (M9).

**SSTables: sorted 4 KB blocks, an index, and a bloom filter.** Each block carries its own
CRC32C, checked on every read, so a flipped bit on disk surfaces as `Corruption` instead
of a wrong value. The index maps the last key of every block to its offset; a lookup
binary-searches it and scans one block. Tables are `mmap`ed, so the page cache is the
block cache and a reader holding a table keeps its mapping alive even after compaction
unlinks the file.

The bloom filter (10 bits per key, 7 probes, ~1% false positives) is what keeps reads of
*absent* keys cheap: without it a miss would read one block from every table.

**Durability needs the directory too.** `fsync` on a file does not persist its directory
entry. A WAL created by a memtable switch is followed by an `fsync` of the directory
before any write lands in it; otherwise an acknowledged write could survive in a file
that no longer exists after a crash. The MANIFEST is replaced with
write-temp, `fsync`, `rename`, `fsync` directory, so a crash leaves the old table set or
the new one.

**Recovery.** Read the MANIFEST (live tables newest-first and the oldest WAL still
needed), replay every newer WAL into a memtable, write that memtable straight to an
SSTable, start a fresh WAL and delete everything the MANIFEST no longer references.
Deleting orphans covers a crash in the middle of a flush: the half-written table was
never in the MANIFEST, so it is garbage.

**Verification.** A model-based test runs 1M random puts, deletes, batches and gets
against both the store and a `std::map`, with a 256 KB memtable so flushes happen
constantly and a full reopen every 200k operations; any divergence fails with the seed.
The crash test runs a writer process that prints each operation only after the store
acknowledges it, `SIGKILL`s it at a random moment, reopens the store and checks every
acknowledged put and delete, for 25 rounds against the same directory.

## Compaction (M2)

Every flush adds a table, and every table is one more place a read may have to look.
Compaction merges tables to bound that read amplification and to reclaim the space held
by overwritten values and tombstones.

**Size-tiered, not leveled.** A run of 4 to 16 tables of similar size (each within 0.5x
to 1.5x of the run's average) is merged into one. Flushes produce equal-sized tables, so
four of them merge into a 4x table, four of those into a 16x table, and so on: the table
count grows with the logarithm of the data size, and each byte is rewritten about
log4(N) times. Leveled compaction (LevelDB, RocksDB's default) gives lower read and space
amplification at the cost of more write amplification. This store's reads are dominated
by the memtable and the bloom filters, while every write is already paid for twice (Raft
log plus WAL), so tiering is the better trade here. Leveled is a stretch goal.

**Only runs that are adjacent in age.** Reads search tables newest-first and stop at the
first hit, so the table order *is* the version order. Merging table 1 with table 3 and
putting the output at position 1 would let table 3's stale value shadow table 2's newer
one. Picking only contiguous runs, and putting the output exactly where the run was,
keeps the order meaningful without storing sequence numbers in SSTables.

**Tombstones are dropped only when the run includes the oldest table.** A tombstone
exists to hide older values of its key. If any table older than the run might still
hold such a value, dropping the tombstone would resurrect it.

**A tombstone-ratio trigger.** Size-tiering alone never merges a small table of
tombstones into the big table holding the data they delete, so a mass delete would not
free space. When tombstones make up 30% of all entries, the store compacts everything
into one table, dropping both the tombstones and the values they shadow.

**Concurrency.** Compaction has its own thread so a long merge never blocks a flush (a
blocked flush would stall writers). The merge runs without the DB mutex: its inputs are
immutable and pinned by `shared_ptr`. Only installing the result takes the lock. Flushes
may have prepended tables in the meantime, but nothing else can change the run, so it is
still contiguous and is replaced in place. Readers holding the old version keep their
tables mapped until they finish, even though the files are already unlinked.

**Crash safety.** The output table is fully written and synced before the MANIFEST that
references it is atomically swapped in, and the inputs are deleted only after that. A
crash mid-compaction leaves an orphaned output file that recovery deletes. The crash test
uses a 16 KB memtable, so its kills regularly land during flushes and compactions.

**Results** (`tests/compaction_test.cc`):

| Check | Result |
|---|---|
| 40k keys deleted, background compaction only | 8.6 MB on disk to 32 bytes (just the MANIFEST) |
| 308 flushes of random overwrites | 98 compactions, peak of 12 live tables |
| 4 readers during constant compaction | no key ever missing, no value ever older than one already seen |

## Networking (M3)

**One wire format for clients and peers.** Every message is a frame:
`u32 length | u8 type | u64 request_id | payload`. A length prefix lets the reader size its
buffer once and never scan payload bytes for a delimiter, and the decoder checks the
length against a limit (64 MB) *before* buffering the body, so a hostile client cannot
make the server allocate gigabytes by sending four bytes. The request id is echoed in the
response, so one connection can have many requests in flight and responses can arrive
out of order. Hand-written rather than Protocol Buffers because the format is five fields
and the decoder is the thing being fuzzed.

**epoll, not a thread per connection.** A thread per connection costs a stack (8 MB of
virtual memory, ~10s of KB resident) and a context switch per request; at 10,000
connections the scheduler, not the store, becomes the bottleneck. Here an accept thread
hands sockets round-robin to a few event-loop threads, each owning its connections and
one `epoll` set. Loops do only non-blocking reads, frame decoding and writes; request
handling runs on a worker pool, so a slow request never stalls the other connections on
its loop.

**Asynchronous responses.** A handler gets the request and a `Responder` callback, and may
call it from any thread at any later time. That shape exists for Raft: a write is
answered only after its log entry commits, which is a different thread, milliseconds
later. The responder posts the encoded response to the owning loop through a mutex-guarded
queue and an `eventfd` wakeup, so a socket is only ever touched by its loop thread.
Responders hold `weak_ptr`s to the connection and the loop: a response for a client that
already hung up, or that completes after shutdown, is silently dropped rather than
written to a recycled file descriptor or a destroyed loop.

**Backpressure and hygiene.** Responses that do not fit in the socket buffer wait in a
per-connection queue and `EPOLLOUT` is armed only while it is non-empty (level-triggered
`EPOLLOUT` on an idle socket would spin the loop). A client that stops reading is cut off
once 64 MB is queued for it. Sockets are `SOCK_CLOEXEC` and `TCP_NODELAY` (requests are
small and latency-bound; Nagle would add up to 40 ms), and writes use `MSG_NOSIGNAL` so a
peer that vanished produces `EPIPE`, not a process-killing `SIGPIPE`.

**Client sessions for exactly-once writes.** Writes carry `(client_id, seq)`; a retry of
the same logical write reuses its `seq`. This is unused by the single-node server but is
designed in now because M7's linearizability check depends on it: if a client's write
times out but actually committed, and the client retries after another client has
written the same key, a naive server applies the old write a second time and a reader
can observe the key go back in time.

**Verification.**

| Check | Result |
|---|---|
| 1,000 simultaneous connections, each doing a PUT and a GET | all correct; server reports exactly 1,000 open, then 0 |
| File descriptors after all clients disconnect and the server stops | equal to the count before it started |
| A frame with an invalid type | that connection is closed; others are unaffected |
| A handler that holds its responder | 50 requests on another connection complete meanwhile |
| libFuzzer on the frame + request/response parsers, 1 hour | 112.8M inputs in 3,601 s, no crash, no leak, peak RSS 550 MB |
| libFuzzer on WAL replay and SSTable parsing | millions of inputs, no crash |

## Raft: the core and the simulator (M4)

**Raft as a deterministic state machine.** `RaftNode` (`src/raft/raft.cc`) owns no
threads, clock or sockets. Its owner calls `Tick()` on a fixed interval, feeds peer
messages to `Step()`, and drains `TakeMessages()` and `TakeCommittedEntries()`. Time is
a tick count and randomness comes from a seeded generator. Two things follow:

- The same code runs in production and in a single-process simulator, so what the
  simulator verifies is the code that ships, not a model of it.
- A failing run replays exactly from its seed, which turns a one-in-a-million
  interleaving into an ordinary failing unit test.

**Persist before you speak.** Every change to the term, the vote or the log goes through
`LogStorage` synchronously, *before* any message that depends on it is queued. A node
that grants a vote, crashes and restarts still remembers it, so it cannot vote for a
second candidate in the same term (which could elect two leaders). If storage fails, the
node aborts: continuing after a failed `fsync` would mean answering peers with state it
may have lost, and fail-stop is the only behavior Raft's proofs cover.

**Elections.** Followers wait a timeout drawn uniformly from 10 to 20 ticks; drawing a
fresh one after every reset makes two nodes timing out together, and splitting the vote
forever, vanishingly unlikely. A node only votes for a candidate whose log is at least
as up to date as its own (last term, then length), which guarantees a new leader holds
every committed entry. A new leader appends a no-op entry in its term: a leader may only
commit entries from its own term by counting replicas (the paper's Figure 8), and the
no-op lets everything before it commit promptly.

**PreVote and CheckQuorum.** Plain Raft has a well-known failure mode: a node cut off by
a partition keeps timing out and incrementing its term, and when the partition heals its
inflated term forces the healthy leader to step down, causing an outage for no reason.

- *PreVote:* a node first asks "would you vote for me in term T+1?" and only bumps its
  term if a majority says yes. A partitioned node never gets that majority, so its term
  stays put (`RaftElection.PreVoteDoesNotChangeTerm`: 1,000 ticks alone, term still 0).
- *CheckQuorum:* a leader that has not heard from a majority within an election timeout
  steps down. A leader stranded on the minority side of a partition stops acting as
  one, and followers that heard from a live leader recently ignore vote requests.

Both matter for M7's "flapping partition" and "leader on the minority side" scenarios.

**The simulator** (`tests/raft_sim.h`) runs 3 or 5 nodes on a logical clock, with a
network that delays (1 to 10 ticks), drops (up to 40%), duplicates (up to 20%) and
reorders messages and can be partitioned arbitrarily, all from one seed. It checks
election safety (one leader per term, ever) after every delivered message, plus
state-machine safety and in-order apply, which M5 starts exercising.

| Test | Result |
|---|---|
| 1,000 randomized runs: crashes, fast crash-restart churn, partitions, loss, duplication, delay | 9,211 terms elected a leader, 0 safety violations, a stable leader after every heal |
| Kill the leader of a 5-node cluster, 50 seeds | a new leader with a higher term every time; the old one rejoins as a follower |
| Isolate a follower for 2,000 ticks, then heal | same leader, same term (PreVote) |

**Testing the tests.** A safety test that has never failed proves little, so the
simulator was checked against deliberately broken Raft:

| Mutation | Caught by |
|---|---|
| Nodes may vote twice in one term | randomized runs, in 4 ms |
| The vote is kept in memory but not persisted | the unit test at once; the randomized runs only after adding fast crash-restart churn (bug log #4) |

## Replication into the LSM tree (M5)

```
client ─► any node ─(NotLeader + hint)─► leader
                                          │ Raft thread: batch proposals, one fsync
                                          ▼
                                 raft.log (FileLogStorage) ──AppendEntries──► followers
                                          │ committed
                                          ▼
                                 KvStateMachine.Apply ─► one atomic WriteBatch into the LSM
                                          │
                                          ▼
                                 responder for that log index ─► client
```

**One thread owns Raft.** Network threads never touch the consensus state. They decode
frames and push events (peer messages, client commands) onto an inbox; the Raft thread
drains it, steps Raft, ticks every 10 ms, proposes all queued client writes as one batch,
sends outgoing messages, applies committed entries and answers the clients waiting on
them. No locks inside the consensus logic, and the code path is the one the simulator
tested.

**Batching is where the throughput comes from.** All client writes that arrive while
the Raft thread is busy become one `ProposeBatch`: one log append, one `fdatasync`, one
`AppendEntries` per follower. This is group commit again, one layer up.

**The durable Raft log** (`FileLogStorage`) is an append-only file of CRC-framed records
(the WAL framing from M1), plus `HARDSTATE` (term and vote) and `SNAPSHOT` metadata
files replaced atomically. Live entries are also cached in memory, since snapshots bound
the log. Truncating a conflicting suffix rewrites the file atomically *before* the new
entries are appended, so a crash can never leave stale entries followed by new ones.

**The state machine stores its applied index in the same write batch as the data.**
Each apply call is one `WriteBatch` holding the data, the client session updates and
`(applied index, term, running hash)`. After a crash the store's contents and its idea of
how far it got cannot disagree, so recovery is: read the applied index, tell Raft, and
let it hand over everything after. The DB itself runs with `sync=false`, because the Raft
log is already durable: anything an unsynced WAL loses in a power failure is re-applied
from the log. That saves a second `fsync` per write. The one rule this creates: the DB
must be flushed before the Raft log is ever compacted past what it has applied (M6).

**Exactly-once writes.** The session table (`client id -> last applied seq`) lives in the
LSM under a reserved key prefix, inside the same atomic batches, so it survives crashes
and is identical on every replica. A retried write whose `seq` was already applied is
acknowledged without being applied again; a unit test shows a retry arriving after
another client's overwrite does not resurrect the old value.

**What a client is told.** A response means the command committed and was applied. If a
proposal's log index ends up holding a different term's entry, it never committed there
and the client is told to retry (safe, thanks to sessions). If it is still pending after 5
seconds the answer is `Timeout`: the outcome is genuinely unknown, and saying so is what
lets M7's checker treat it correctly.

**Reads, for now, go through the log.** A GET is proposed like a write and answered when
it is applied, which is linearizable but costs a log append and an `fsync`. M6 replaces
it with ReadIndex.

**Verification.**

| Test | Result |
|---|---|
| Simulator: 500 randomized runs with a write stream, crashes, partitions, loss | 43,862 entries applied identically on every node, 0 violations |
| Simulator: every minority subset of a 3- and 5-node cluster lost, leader included | every write before and during the loss present on every node afterwards |
| Real cluster: leader stopped under 4 concurrent writers, then restarted | 1,737 acknowledged writes all readable; nodes converge to the same applied hash |
| Real cluster: 2 of 5 nodes down (one the leader) | writes keep succeeding; the two catch up when restarted |
| Real cluster: every node stopped and restarted | all data readable |

**Mutation testing, round two.** Three classic Raft bugs, introduced one at a time:

| Mutation | Caught by |
|---|---|
| Vote without checking the candidate's log is up to date | randomized runs: "index 57 applied as term 1 elsewhere but term 10" |
| Follower sets commit to the leader's commit, not `min(leader commit, last new entry)` | only after two test changes (bug log #7): "node 2 committed through index 2 but its log ends at 1" |
| Leader commits an earlier term's entry by counting replicas (the paper's Figure 8) | only after harsher fault schedules (bug log #7): "index 29 applied as term 8 elsewhere but term 12 on node 1" |

## Snapshots, linearizable reads and rejoining (M6)

**Log compaction.** Once 10,000 entries have been applied since the last snapshot, a node
flushes its LSM tree (the DB runs unsynced, so this is what makes the applied state
durable) and then drops all but the last 1,000 entries from its Raft log. Keeping a tail
lets a follower that is briefly behind catch up from the log instead of needing a full
snapshot. There is no separate snapshot file: the LSM tree *is* the snapshot, and a
serialized copy is produced only when a follower actually needs one.

**InstallSnapshot.** When a follower needs entries the leader has compacted away, the
leader streams the state machine to it in 1 MB chunks, stop-and-wait: the next chunk
goes out only when an acknowledgement *advances* the offset, and a chunk is resent only
after going unanswered for half an election timeout. On the follower, the state machine
restores the snapshot durably *before* Raft resets its log. A crash in between leaves a
state machine ahead of its log, which recovery handles; the other order could leave a
log that starts after the state machine ends, which nothing can repair.

The restore builds the new LSM tree in a side directory and swaps it in with two
renames. If the process dies between them, startup finds the new tree and finishes the
swap; if it dies before them, the half-built tree is discarded.

**ReadIndex instead of reading through the log.** In M5 a GET was a log entry: correct,
but every read paid for a disk append, an `fsync` and replication. Now the leader records
its commit index as the read index, sends one heartbeat round to confirm it is still the
leader, waits until it has applied through the read index, and reads locally. All GETs
that arrive in the same loop iteration share one heartbeat round. Two details carry the
correctness:

- A new leader does not serve reads until its own no-op has committed. Before that, its
  commit index may lag entries a previous leader already committed.
- Confirmation needs a response from a majority in the leader's *current* term. A leader
  stranded on the minority side of a partition never gets one, so it never answers.

**Rejoining with a wiped disk.** Raft assumes stable storage. A node whose disk is
replaced has forgotten its votes and possibly entries it acknowledged, so it could vote
twice in one term, or help elect a leader that is missing committed entries. Restarted
with `--join`, a node stays a silent follower (no voting, no campaigning) until it has
committed an entry from the current leader's term. By Log Matching its log then contains
every entry committed before it, and it votes normally from then on. The joining flag is
stored durably next to the term and vote, so a crash before catching up does not quietly
turn it back into a voter.

**Verification.**

| Test | Result |
|---|---|
| Simulator: 2,000 randomized runs with compaction every 5 to 25 entries, multi-chunk snapshots, disk wipes, crashes, partitions, loss, and a read stream | 20,469 snapshots installed, 119,808 reads checked, 0 violations |
| Simulator: every read's index vs. the highest commit index anywhere when the read was issued | never lower (no stale read) |
| Simulator: leader partitioned with one follower of five, read issued | never confirmed |
| Real cluster: node wiped and restarted with `--join` after the leader compacted past index 400 | installs a snapshot, rejoins as a voter, has every key |
| Real cluster: 2,000 writes with a 100-entry threshold, then a full restart | log stays within 110 entries of the snapshot; data intact |
| Real cluster: 200 GETs | the leader's last log index does not move |

## Fault injection and linearizability checking (M7)

Unit tests and the simulator exercise the Raft core; this milestone attacks the real
binary, over real TCP, with real disks, and checks what clients observed.

```
 kvhistory (8 clients) ──► 3-node Docker cluster ◄── chaos/run.sh: kill -9, iptables,
      │ invoke/return times                            tc netem, wipe, libfaketime
      ▼
 history.jsonl ──► Porcupine (per-key register model) ──► linearizable / violation + HTML
```

**The history.** Eight concurrent clients issue random GETs, PUTs and DELETEs over ten
keys, recording each operation's invoke and return time. Every written value is unique,
so a read pins down exactly which write it observed. A write that timed out or lost its
node has an unknown outcome: it may have been applied at any point after its call, or
never. The checker models that by letting it return at infinity. Reads go to a random
node (see bug log #12), so a node that answers from stale state is exercised, not just
the leader.

**The checker** is Porcupine with a register model, partitioned by key. Keys are
independent registers, and checking each key's sub-history separately keeps the search
exponential only in per-key concurrency. A typical 30,000-operation history checks in
well under a second. Its own unit tests feed it legal histories and four kinds of
illegal ones (stale read, a value from nowhere, a resurrected write, an out-of-order
write) before it is trusted with anything.

**Faults** (`chaos/run.sh`; each scenario reruns from a seed):

| Scenario | Injection |
|---|---|
| Leader crash | `docker kill -s KILL` the leader mid-write, twice, restarting it each time |
| Follower crash and restart | kill a follower, wait 10 s, restart it with its data |
| Minority partition | `iptables` drops all traffic between one follower and the others |
| Leader on the minority side | the same, applied to the leader |
| Flapping partition | heal and isolate a random node every 2 s for 60 s |
| Slow network | `tc netem delay 300ms 200ms` on every node (100 to 500 ms per hop) |
| Packet loss | `tc netem loss 10%` on every node |
| Data wipe | stop a follower, delete its data, restart it with `--join` after the leader has compacted |
| Clock skew | node3 runs under libfaketime at 2x clock speed; the leader is cut off twice to force elections |

The chaos cluster runs with 50 ms ticks (750 to 1500 ms election timeouts). The election
timeout has to exceed the worst round trip the network will produce, and the
slow-network scenario injects up to a second of it; at the default 150 to 300 ms the
cluster would hold elections continuously, which is correct behavior but not a useful
test.

**Validating the harness.** A harness that never reports a failure proves nothing, so
each fault was checked to have actually happened:

| Check | Evidence |
|---|---|
| A broken server is caught | `UNSAFE=1` starts nodes that answer GETs from local state with no ReadIndex. Porcupine reports the history illegal and writes a visualization of the violation |
| The wiped node really rebuilt from a snapshot | its status after the run shows `snapshots_installed=1`, `joined=1`, and the same apply hash as the other two |
| The clock skew was real | node3's Raft tick counter ends at 2.00x node1's; a run where the ratio is under 1.6 is reported inconclusive, not passed |
| The network faults bit | throughput drops from ~30,000 operations per run to ~15,000 to 20,000 under netem, and the flapping run moves through a dozen terms |

**Results** (`chaos/matrix.sh 1 20`, every scenario with seeds 1 to 20):

| Scenario | Runs | Linearizable | Violations | Inconclusive | Operations checked |
|---|---:|---:|---:|---:|---:|
| leader-crash | 20 | 20 | 0 | 0 | 531207 |
| follower-restart | 20 | 20 | 0 | 0 | 481514 |
| minority-partition | 20 | 20 | 0 | 0 | 463657 |
| leader-minority | 20 | 20 | 0 | 0 | 435379 |
| flapping-partition | 20 | 20 | 0 | 0 | 312578 |
| slow-network | 20 | 20 | 0 | 0 | 325159 |
| packet-loss | 20 | 20 | 0 | 0 | 387237 |
| data-wipe | 20 | 20 | 0 | 0 | 937289 |
| clock-skew | 20 | 20 | 0 | 0 | 468751 |
| **Total** | **180** | **180** | **0** | **0** | **4,342,771** |

Zero linearizability violations in 180 runs and 4,342,771 client operations. Every
run's history, harness log and verdict is reproducible from its scenario and seed.

## Bug log

| # | Milestone | Symptom | Cause | Caught by |
|---|---|---|---|---|
| 1 | M1 | UBSan: `null pointer passed as argument 2` in `MemTable::Add` | A delete's empty value is a `string_view` with a null `data()`, and `memcpy(dst, nullptr, 0)` is undefined behavior even for zero bytes | UBSan in the ASan build |
| 2 | M1 | ASan `stack-use-after-scope` and a TSan race in the concurrent-reader test | The reader lambda captured the loop index by reference; it was read after the loop moved on | ASan and TSan, same run |
| 3 | M3 | Frame fuzzer died with `SIGTRAP` after ~800 inputs | Not a decoder bug: the fuzz target asserted that a decoded request re-encodes to identical bytes, but varints have non-minimal encodings (`80 00` is also 0). The real invariant is decode, encode, decode gives the same fields | libFuzzer |
| 4 | M4 | The randomized simulator passed with vote persistence deliberately removed | Faults were only injected every 50 to 200 ticks; a forgotten vote only elects two leaders if a node votes, crashes, restarts and votes again *within one election*. Added per-tick crash-restart churn; the mutation then fails at seed 5, tick 1425: "nodes 1 and 2 both leader in term 8" | Mutation testing |
| 5 | M4 | ASan heap-use-after-free in a Raft unit test; release builds passed | The test kept a pointer into the vector returned by `TakeMessages()`, a temporary destroyed at the end of the statement | ASan and TSan |
| 6 | M4 | CI lint failed on code that "passed" lint locally | Local clang-tidy colorizes output, so `grep ' error: '` matched nothing and the local gate passed silently since M2. Lint now runs with `--use-color=false` and is judged by exit code | CI |
| 7 | M5 | Two deliberately injected Raft bugs passed 500 randomized runs | Every append carried the follower's whole missing log (tiny commands, 1 MB batches), so "commit past what the leader vouched for" never had a partial append to bite; and leaders rarely died with entries half-replicated, which Figure 8 needs. Half the runs now send one entry per message, leaders are killed on purpose, churn is 3x higher, and the simulator checks that commit never passes the end of a node's log | Mutation testing |
| 8 | M6 | A wiped follower never caught up; the leader resent the same append forever | The leader clamps its retry point to `match + 1`, assuming follower logs never shrink. A wiped disk breaks that. A rejection now lowers `match`, which is always safe | Simulator |
| 9 | M6 | Randomized snapshot runs took seconds each and looked hung | Each heartbeat resent the current snapshot chunk, and the ack for every resend started another chain of chunks, so concurrent transfers multiplied for as long as the snapshot was in flight. Now strictly stop-and-wait: only an ack that advances the offset sends more, and retransmission waits for a timeout | A hung test, then simulator message tracing |
| 10 | M6 | State machine safety violation: "index 178 applied as term 2 elsewhere but term 3 on node 2" | A wiped node in join mode crashed and restarted before catching up; join mode was a startup flag, so it came back as a voter with an empty log and helped elect a leader missing committed entries. The joining flag is now persisted with the term and vote | Simulator |
| 11 | M7 | The clock-skew scenario passed without skewing anything: node3 ticked exactly as often as node1 | Two causes. libfaketime 0.9.10 leaves `CLOCK_MONOTONIC` alone by default, and `steady_clock` reads it. With monotonic faking on, the Raft loop's condition-variable `wait_until` hung forever, because `pthread_cond_clockwait` is not intercepted. Fixed by faking the monotonic clock, replacing the condvar with an eventfd and `poll()` (which libfaketime scales), and having the harness assert node3's tick rate (now 2.00x) | Checking that the fault happened |
| 12 | M7 | A deliberately broken server (stale local reads) passed the linearizability check | The recorder sent every read to the leader, so stale followers were never asked. CheckQuorum also shrinks a deposed leader's window to under a second, during which no newer write can commit. Reads now go to a random node; the broken server is flagged at once | Negative control |
