# cooperdb

A log-structured key/value storage engine written in Go, implementing the
[Bitcask](https://riak.com/assets/bitcask-intro.pdf) design.

A cooper is the craftsman who makes casks — a nod to Bitcask, which this
follows closely. Work in progress.

Every write appends to the end of a log file rather than updating in place, so
writes stay sequential. An in-memory map holds the file and byte offset of each
key's newest record, which keeps a read to one map lookup plus one seek. The
tradeoff: every key lives in RAM, so the keyspace is bounded by memory.

## What works

```go
db, err := cooperdb.Open("./data")
err = db.Put([]byte("user:1"), []byte("alice"))
value, err := db.Get([]byte("user:1"))   // ErrKeyNotFound if absent
err = db.Delete([]byte("user:1"))
err = db.Merge()                         // reclaim space from dead records
err = db.Close()
```

- Binary record format with a CRC-32 checked on every read
- Append-only data file; reads by absolute offset, so the disk read itself needs
  no lock
- In-memory keydir mapping each live key to its newest record
- `Put` / `Get` / `Delete`, the last writing a tombstone rather than erasing
- A configurable flush policy, measured below
- Recovery on `Open` — the keydir is rebuilt from the log, so data survives a
  restart
- File rotation — the active file is sealed past a size threshold and reads
  resolve across every file
- Compaction — `Merge` rewrites the sealed files, keeping only what is still
  live, and deletes the originals
- Safe for concurrent use — reads run in parallel, and `Merge` runs alongside
  reads and writes
- Crash-tested — 300 consecutive `SIGKILL`s, including 89 in the middle of a
  merge, with zero acknowledged writes lost

Not yet: `Merge` has to be called by hand.

## File rotation

Writes go into one *active* file. Once it passes a size threshold the file is
sealed and a new one started, so a database is a numbered sequence of files —
`000000.data`, `000001.data`, and so on — of which only the last is written to.

```go
db, err := cooperdb.Open("./data", WithMaxFileSize(64<<20))   // 64 MiB
```

The default is 2 GiB, which is also Bitcask's. Sealed files are immutable, and
that immutability is what compaction works on — a single ever-growing file would
leave it nothing safe to rewrite.

**The threshold should be comfortably larger than your typical record.** A
record cannot be split across files, so one that exceeds the threshold gets a
file to itself. Set the threshold near or below your record size and you get
roughly one file per record, which is a great many files and a great many open
descriptors.

Three decisions worth knowing about, because they are visible in how the
database behaves:

**The threshold is checked before a record is written, not after.** So a file
never exceeds it, rather than overshooting by one record — except in the
oversized case above, where there is no alternative.

**Sealing flushes.** A file that will never be written again is flushed to disk
as it is sealed, whatever the sync policy says. This bounds how much unflushed
data can accumulate, and it is why the `SyncNever` row below says *since the
last file seal* rather than something unbounded.

**Sealed files are held open, and opened lazily.** A read resolves which file
holds the record and keeps that handle for subsequent reads. A restart therefore
opens exactly one file — the active one — and older files are opened only when a
read actually reaches for them. Handles are opened read-only, so appending to a
sealed file is refused by the kernel rather than merely discouraged.

The cost is that nothing evicts them: a database with very many files will hold
very many descriptors. A production version would bound that with an LRU cache;
Basho's own guidance for Bitcask is to raise the open-file limit instead.

The threshold is a runtime setting, not a property of the data. Reopening with a
different value is fine — existing files keep whatever size they were written at.

## Compaction

An append-only log only grows. Overwriting a key leaves the old record on disk,
and deleting one *adds* a record. `Merge` reclaims that space: it reads every
sealed file, writes only the records still being served into a new file, and
deletes the originals.

```go
err = db.Merge()
```

On a database of 200 keys each written four times with a tenth deleted:

| | files | bytes |
|---|---:|---:|
| before | 41 | 41,300 |
| after | 2 | 9,680 |

**Whether a record survives is decided by the keydir, not by reading the log.**
For each record, merge asks whether the keydir still points at *this* file and
*this* offset. Matching on location rather than on timestamps makes it an exact
identity check — a file id plus a byte offset names one record and nothing else,
because file ids are allocated from a counter that never reuses one.

**Merge output takes a fresh, high file id.** That inverts the usual assumption
that a higher-numbered file holds newer data, which is exactly why replay orders
by timestamp rather than by arrival — see Recovery below. Three things had to be
true before this was safe: timestamps are unique and monotonic, tombstones carry
their timestamp through replay, and `Open` starts a fresh active file instead of
appending into whichever file has the highest id.

**The merged file is flushed before any original is deleted.** That ordering is
the whole crash-safety story. Delete first and a power loss takes both copies at
once — the new file still sitting in the page cache, the old ones already gone.
If the flush fails, the output is discarded and the inputs are left untouched, so
a failed merge costs nothing and can simply be retried.

**A crash mid-merge leaves nothing to clean up by hand.** The output is written
under a temporary name, `NNNNNN.merge.tmp`, and renamed to `.data` only once it
has been flushed. A rename is atomic, so after a crash the output is either
missing, complete, or still carrying its temporary name — and `Open` deletes any
`.merge.tmp` it finds. A crash *after* the rename but before the originals are
deleted leaves two copies of some records, which is harmless: the copies carry
their original timestamps, so replay resolves them correctly, and the next merge
folds them together.

### Known limits

**Tombstones are never dropped.** A tombstone can only be discarded once every
older record for its key is gone too, and that is not yet checked — so merge
copies all of them forward, every time, and **space held by deletions is never
reclaimed**. Delete-heavy data ends up with merged files that are mostly
tombstones.

**There is no trigger.** Merge rewrites every sealed file whether or not there is
anything to reclaim, so merging twice in a row does the second pass for nothing.
Fine while `Merge` is called deliberately; it would be wasteful on a timer.

**Output is a single file**, ignoring the rotation threshold, so a large database
merges into one oversized file.

## Concurrency

A `*DB` is safe to share between goroutines. There is no need to wrap it in your
own mutex.

### What runs at the same time

| these two | at the same time? |
|---|---|
| `Get` + `Get` | **yes**, fully in parallel |
| `Get` + `Put` / `Delete` | **they take turns** — a read waits for a write in progress |
| `Put` / `Delete` + `Put` / `Delete` | **no** — writes go one at a time |
| `Merge` + `Get` | **yes** |
| `Merge` + `Put` / `Delete` | **yes** |
| `Merge` + `Merge` | **no** — the second returns `ErrMergeInProgress` |

**Reads run in parallel.** Any number of `Get` calls can be in progress at once.
Each one looks up the key under a shared lock, then reads the value from disk
with no lock held at all.

**Writes run one at a time, and a read waits for a write in progress.** A `Put`
or `Delete` holds an exclusive lock for its whole duration — the append to the
file *and* the flush your sync policy asks for. Under `SyncNever` that is a few
microseconds; under `SyncAlways` it is a full `fsync`, and reads arriving in that
window wait for it.

**Writes cannot be starved by reads.** Once a write is waiting, Go's
`sync.RWMutex` stops letting new reads in ahead of it. A constant stream of reads
therefore never locks out writes — at the cost that reads briefly queue behind a
waiting write.

**`Merge` runs alongside both.** It copies records without holding the lock, so
reads and writes carry on throughout. They are only held up for the final step,
when the in-memory index is repointed at the new file — a memory-only operation
with no disk I/O.

**A read does not fail because a merge deleted its file.** If a merge removes
the file a read was about to use, the read looks the key up again — up to three
attempts — and finds the merged copy. This works because a merge always updates
the index *before* it deletes anything, so the second lookup already points at
the new file.

### One merge at a time

Only one `Merge` runs at once. A second call made while one is running returns
`ErrMergeInProgress` straight away and does nothing:

```go
err := db.Merge()
if errors.Is(err, cooperdb.ErrMergeInProgress) {
	// another merge is already running; nothing to do
}
```

It returns rather than waiting on purpose. By the time the first merge finishes,
the files are already compacted, so a second pass would rewrite everything for no
gain.

### Closing

Stop every goroutine that uses the database, *then* call `Close`:

```go
var wg sync.WaitGroup
// ... goroutines that use db, each calling wg.Done() when finished
wg.Wait()

err := db.Close()
```

What `Close` guarantees:

- **It waits for a running `Merge` to finish**, so nothing is written to or
  deleted from the directory after `Close` returns.
- **It waits for a write in progress**, and flushes it.
- **A read already in progress** either completes normally or returns
  `ErrDatabaseClosed` — never a partial value.
- **Every call after `Close` returns `ErrDatabaseClosed`**, including `Get`. No
  file is reopened after shutdown.
- **Calling `Close` twice is safe.** The second call returns `nil`.

`Close` does **not** wait for goroutines that have not yet made their call. That
part is the caller's job, which is why the `WaitGroup` comes first.

If the database has already failed — a flush returned an error, see Durability —
`Close` still releases every file but does not flush again, and returns that
original error. Errors from later calls match both:

```go
errors.Is(err, cooperdb.ErrDatabaseClosed) // true
errors.Is(err, originalFlushError)         // also true
```

### Errors

| error | returned by | meaning |
|---|---|---|
| `ErrMergeInProgress` | `Merge` | another merge is already running; this call did nothing |
| `ErrDatabaseClosed` | every call after `Close` | the database has been closed |

### How this is tested

The concurrency tests run readers, writers and merges at the same time under
Go's race detector:

```sh
go test -race ./...
```

Each lock was also removed one at a time to confirm that a test fails without
it. A race-detector run only reports races it actually observes, so a passing
suite on its own does not prove the locks are needed.

## Recovery

The keydir lives only in memory. `Open` reconstructs it by replaying every data
file in creation order, which is what makes a write survive the process that
made it.

Replay walks files in creation order and records in offset order. Filenames are
zero-padded (`000009.data`, `000010.data`) precisely so a text sort gives that
order.

**But arrival order is not what decides the winner — the timestamp is.** Every
record carries one, and a record older than the entry already held is skipped.
That matters because compaction rewrites old records into *newly created* files,
so a high file id no longer implies recent data: a relocated record from 2019 can
be replayed after a write from this morning, and must lose.

Timestamps are assigned monotonically rather than straight from the clock —
`max(now, last + 1)` — so no two records can share one and none can go backwards
across a restart or an NTP step. That makes the comparison a total ordering, and
a total ordering is what lets merge put its output wherever it likes.

**A tombstone is held during replay rather than removing its key immediately.**
It becomes a keydir entry carrying the deletion's timestamp, and those entries
are swept before `Open` returns, so the keydir you get holds live keys only and
costs nothing extra at rest.

The obvious alternative — delete the key the moment a tombstone is replayed —
has a subtle flaw. Deleting leaves no timestamp behind, so an older record for
that key arriving later finds nothing to compare against and wins: the deleted
key comes back holding a stale value, with no error anywhere. Riak's Bitcask
carried that same bug for years ([issue #82](https://github.com/basho/bitcask/issues/82)),
and fixing it there needed a new on-disk tombstone format. It was cheap to fix
here only because a tombstone is an ordinary record with a flag set, so it was
already carrying a timestamp — the information was on disk all along, and only
the in-memory side was throwing it away.

**A damaged tail ends the scan rather than failing the open.** A process killed
mid-write leaves a partial record at the end of the active file; everything
before it is intact, so recovery takes what it can read and stops. Treating that
as corruption would make a single ungraceful shutdown render the database
permanently unopenable.

**And the damage is then removed**, before anything is appended. `Open`
truncates the active file to the length the scan could actually read, rather
than to the length the filesystem reports — those differ by exactly the size of
the partial record.

That distinction matters more than it looks. Records carry no start marker: each
one is found by reading the previous one's header and stepping forward by its
length. Append *past* a partial record rather than over it, and the chain
breaks — every record written afterwards is durably on disk and permanently
unfindable, because the next scan stops at the same damaged byte and can never
reach them. The loss then surfaces one restart *after* the crash that caused it.

A write that fails partway is cleaned up the same way, immediately: the partial
bytes are truncated away before `Put` returns its error, so the file and the
in-memory write offset never disagree.

### Known limit: damage in the middle of a file

Recovery handles a damaged **tail**, which is the shape a crash produces.
Damage in the **middle** of a file — bit rot, a bad backup restore, a truncated
copy — is not recoverable: the scan stops there, and every record after it
becomes unreachable.

The reason is the same chaining property. Finding the next record after
unreadable bytes would mean guessing offsets and CRC-checking each until one
validated, and this format has no support for that. Bitcask has the same limit.

In practice this needs damage cooperdb did not cause, since both the crash case
and the failed-write case are truncated away before anything is written past
them.

## Crash testing

Recovery is only proven by crashing something. The crash test starts a second
process that writes, deletes and merges as fast as it can, kills it with
`SIGKILL` at a random moment, reopens the database, and checks every operation
the killed process was promised. Then it starts another process on **the same
directory** and does it again.

```sh
go test -race -run TestAcknowledgedWritesSurviveRepeatedKills -v ./...    # 20 kills

COOPERDB_CRASH_RUNS=300 \
  go test -race -run TestAcknowledgedWritesSurviveRepeatedKills -v ./...  # 300 kills
```

`SIGKILL` is the hardest kill there is: no `Close`, no deferred code, no chance
to clean up. It runs as part of `go test ./...` and is skipped under `-short`.

### How it knows what is correct

The child process announces every operation twice. It prints `intent` just
before calling `Put` or `Delete`, and `ack` only after the call returns nil:

```
CRASH intent put key17 key17@g5s4521
CRASH ack    put key17 key17@g5s4521
```

An `ack` is a promise: that operation succeeded and must survive. Everything the
child printed is still in the pipe after it dies, so the parent sees every
promise made before the kill.

The child performs one operation at a time, so when it is killed **at most one
operation is in flight**: the last `intent` with no `ack`. That gives every key
exactly two correct states after the reopen: what the last acknowledged
operation left, or what the interrupted one would have left. Anything else
fails the test — a lost write, a deleted key coming back, a value nobody wrote.
The test also counts every key in the database, so a key appearing from nowhere
fails it too.

The interrupted operation really does land without its `ack` sometimes, because
the kill can arrive between `Put` returning and the line being printed. A test
that expected only the acknowledged state would fail a correct database.

### Why the same directory, and why merges

**Repeated kills on one directory**, because some bugs only show up a restart
*after* the crash that caused them. Each child starts on whatever the previous
crash left behind, and each check covers everything written since the first
one, not only the last run. Every value carries its generation number
(`key17@g5s4521`), so a value left over from an earlier run can never pass for
a newer one.

**Merges**, because they are the only thing that copies old records into a
newer file — which is exactly where the dangerous recovery bugs live. A
tombstone replayed before an older, relocated copy of its key is how a deleted
key comes back. Without merges, older records always sit in older files, so even
broken recovery code gets the right answer. The child uses 1 KB files so
rotation and merging happen constantly, and one generation in three merges
every 20 operations. Merging in every generation would put nearly every kill
inside a merge, because a merge takes far longer than a write.

### Results

300 consecutive kills on one directory, under the race detector:

| | |
|---|---:|
| acknowledged operations verified | 28,366 |
| merges completed | 36 |
| kills that landed mid-merge | 89 |
| abandoned `.merge.tmp` files swept by `Open` | 82 |
| half-written records found | 0 |
| acknowledged operations lost | **0** |

Where the interrupted operation stood at each kill:

| did not land | landed without its `ack` | no visible change | nothing in flight |
|---:|---:|---:|---:|
| 197 | 6 | 2 | 95 |

"Did not land" dominates because each file fills after about 25 writes and
sealing it forces an `fsync`, which takes milliseconds. Most kills arrive during
that flush, and the size check runs *before* a record is appended, so the
interrupted write has not reached the file yet.

### Proof it can fail

A test that has never failed has not been shown to catch anything, so three
real recovery bugs were planted, one at a time:

| planted bug | first failure |
|---|---|
| replay ignores timestamps | `gen 3: key18 = "<absent>", want "key18@g3s25"` |
| replay forgets a tombstone's timestamp | `gen 12: key28 = "key28@g9s57", want "<absent>"` |
| merge deletes its inputs before renaming its output | `gen 9: key53 = "<absent>", want "key53@g9s26"` |

The second is the deleted-key bug described under Recovery. Here it surfaced
**three crashes after the value was written**: `key28` was written in
generation 9, deleted later, and came back in generation 12. An earlier version
of this test, with a fresh directory per kill and no merges, passed **50 runs
out of 50** with that same bug in place. Repeated kills and merges are what make
it visible.

### What it does not prove

- **Power loss.** `SIGKILL` kills the process, not the machine. Anything `write`
  handed to the kernel stays in its page cache and still reaches the disk, so all
  three sync policies behave identically here. Testing power loss needs a VM
  that can be hard-reset, or block-level fault injection. See Durability.
- **Torn records.** None of the 300 kills produced one. Each record is written
  with a single `write` call, and a kill cannot split a write to a regular file.
  The torn-tail recovery described above is covered by separate tests that
  damage the file directly: one cuts the last record short, another appends a
  partial record after it.

## Record format

All integers little-endian.

| Offset | Size | Field |
|--------|------|-------|
| `0`  | 4 | CRC-32 (IEEE), over every byte from offset 4 to the end of the record |
| `4`  | 8 | timestamp, `int64` Unix nanoseconds |
| `12` | 4 | key size, `uint32` |
| `16` | 4 | value size, `uint32` |
| `20` | 1 | flags; bit 0 marks a tombstone, seven bits free |
| `21` | *key size* | key |
| `21 + key size` | *value size* | value |

The 21-byte header is fixed, so a reader reads the header first, learns the two
sizes, then reads exactly the rest of the record.

A tombstone is marked by the flag, not by an empty value — storing an empty
value is a legitimate write, and the two have to stay tellable apart when the
log is replayed. The flag sits inside the checksum's range, so a bit flipped on
disk fails the CRC instead of silently deleting a key.

## Durability

Flushing is chosen at `Open` and defaults to `SyncNever`:

```go
db, err := cooperdb.Open("./data")                          // SyncNever
db, err := cooperdb.Open("./data", WithSyncEveryN(100))
db, err := cooperdb.Open("./data", WithSyncAlways())
```

One writer:

| policy | ops/sec | ns/op | a power loss can take |
|---|---:|---:|---|
| `SyncNever` | ~666,000 | 1,501 | writes since the last file seal that the kernel has not written back yet |
| `SyncEveryN(100)` | ~26,200 | 38,165 | at most the last 100 writes |
| `SyncAlways` | ~268 | 3,729,104 | nothing that has returned |
| `Get` (policy has no effect) | ~2,650,000 | 378 | — |

Durability costs about **2,500×** on this hardware.

**Every row above describes power loss or a kernel panic.** Under a *process*
crash — a panic, a `SIGKILL` — all three policies lose nothing that `Write`
accepted, because the page cache belongs to the kernel and outlives the
process. A crash harness built on `SIGKILL` therefore cannot tell these three
apart; separating them needs a hard-reset VM or block-level fault injection.

`Close` flushes whatever the policy says, so a clean shutdown never loses
writes. A failed flush is never retried — a failed `fsync` can leave the kernel
treating the pages as clean, so a second call would report success over data
that is already gone. The database is marked failed instead and refuses all
further operations, reads included.

### Concurrent writers

The table above uses a single goroutine, so nothing ever waits for the lock.
With several goroutines writing at once, every `Put` takes its turn at the
write lock, and the picture changes in ways a single writer cannot show. The
latencies below are per `Put`, **time spent waiting for the lock included**.

`SyncAlways`:

| writers | writes/sec | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 268 | 3.97 ms | 5.12 ms |
| 10 | 261 | 39.0 ms | 42.0 ms |
| 100 | 279 | 359 ms | 406 ms |

`SyncNever`:

| writers | writes/sec | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 729,824 | 1.38 µs | 2.58 µs |
| 10 | 484,742 | 1.58 µs | 348.5 µs |
| 100 | 482,431 | 1.67 µs | 1,997 µs |

**When every write is synced, adding writers adds no throughput.** `Put` holds
the write lock across the `fsync`, so writes go through one at a time at about
3.7 ms each: roughly 270 a second in total, however many goroutines are
writing. Each extra writer only makes the queue longer. With 100 writers, a
write waits about 0.4 s. Databases avoid this with *group commit*, where a
single flush makes every waiting writer's record durable at once. cooperdb does
not do that yet.

**Without syncing, contention costs a third of the throughput and shows up in
the tail.** Going from one writer to ten drops throughput from ~730K to ~485K
writes/sec, and it then holds flat up to 100. The typical write barely changes
(1.4 → 1.7 µs), but the slowest 1% becomes about **770× slower** (2.6 µs → 2.0
ms). An average would hide that entirely.

**Uncontended, the locks cost very little.** Measured against the code from
just before they were added, back to back on the same machine, a lone `Put` is
6.8% slower and a `Get` 2% slower. An uncontended lock and unlock on its own
costs about 6 ns.

### How these were measured

```sh
# one writer: fixed count for the fsync-bound rows, 2-second runs for the fast ones
go test -bench='PutSyncEveryN|PutSyncAlways' -benchtime=10000x -count=5 -run='^$' ./...
go test -bench='PutSyncNever|BenchmarkGet'   -benchtime=2s     -count=6 -run='^$' ./...

# concurrent writers
go test -bench='PutWriters/SyncAlways' -benchtime=33000x         -run='^$' ./...
go test -bench='PutWriters/SyncNever'  -benchtime=2s    -count=5 -run='^$' ./...
```

- Apple M5 MacBook Air (10 cores, no fan), macOS 26.6.2, Go 1.26.6,
  darwin/arm64
- Local NVMe SSD, APFS, via `b.TempDir()`
- 100-byte values, no competing I/O; medians reported where runs were repeated
- **Run length depends on the benchmark.** fsync-bound benchmarks use a fixed
  number of writes: 10,000 for one writer, and 33,000 per concurrent case
  (about 2 minutes each), so p99 rests on hundreds of samples rather than a
  handful. Fast benchmarks run for 2 seconds and are repeated. At ~1.5 µs a
  write, 10,000 iterations is only 15 ms, which never gets past the machine
  warming up — an earlier version of this table measured that way and
  understated `SyncNever` by about 20%.
- The concurrent benchmark overwrites a fixed set of 10,000 keys, so the keydir
  stays the same size for the whole run. Percentiles are nearest-rank.

Read these as comparisons rather than as absolute throughput:

- **`Get` never touches the disk here.** Its 1,000-key working set is ~130 KB
  and stays in the page cache for the whole run, so 378 ns is a memory read. A
  dataset larger than RAM would pay real I/O per lookup.
- **`fsync` cost is a property of the storage.** On macOS Go issues
  `F_FULLFSYNC`, which waits for the drive to flush its own write cache. Linux's
  default `fsync` often returns earlier, so published numbers elsewhere are
  usually measuring a weaker guarantee.
- **`ns/op` in the one-writer table is a mean.** Under `SyncEveryN(100)`, 99
  writes are fast and the 100th absorbs a full flush; the average hides a
  latency no single operation actually experiences. The concurrent-writer
  tables report percentiles for that reason.
- **No readers run during the concurrent-writer benchmark.** Reads alongside
  writes, and the effect of Go's `RWMutex` making new reads wait behind a
  queued writer, are not measured yet.

## Testing

```sh
go test -race ./...
```

`-race` matters here: the concurrency tests only prove anything under the race
detector.
