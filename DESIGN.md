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

## Bug log

| # | Milestone | Symptom | Cause | Caught by |
|---|---|---|---|---|
| 1 | M1 | UBSan: `null pointer passed as argument 2` in `MemTable::Add` | A delete's empty value is a `string_view` with a null `data()`, and `memcpy(dst, nullptr, 0)` is undefined behavior even for zero bytes | UBSan in the ASan build |
| 2 | M1 | ASan `stack-use-after-scope` and a TSan race in the concurrent-reader test | The reader lambda captured the loop index by reference; it was read after the loop moved on | ASan and TSan, same run |
