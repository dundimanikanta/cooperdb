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
err = db.Close()
```

- Binary record format with a CRC-32 checked on every read
- Append-only data file; reads by absolute offset, so they take no lock
- In-memory keydir mapping each live key to its newest record
- `Put` / `Get` / `Delete`, the last writing a tombstone rather than erasing
- A configurable flush policy, measured below
- Recovery on `Open` — the keydir is rebuilt from the log, so data survives a
  restart
- File rotation — the active file is sealed past a size threshold and reads
  resolve across every file

Not yet: compaction and concurrent access.

## File rotation

Writes go into one *active* file. Once it passes a size threshold the file is
sealed and a new one started, so a database is a numbered sequence of files —
`000000.data`, `000001.data`, and so on — of which only the last is written to.

```go
db, err := cooperdb.Open("./data", WithMaxFileSize(64<<20))   // 64 MiB
```

The default is 2 GiB, which is also Bitcask's. Sealed files are immutable, and
that immutability is what compaction will need to work on — a single
ever-growing file would leave it nothing safe to rewrite.

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

## Recovery

The keydir lives only in memory. `Open` reconstructs it by replaying every data
file in creation order, which is what makes a write survive the process that
made it.

Replay walks files in creation order and records in offset order, so the last
record to touch a key is the one that wins. Filenames are zero-padded
(`000009.data`, `000010.data`) precisely so a text sort gives that order. A
tombstone removes its key instead of storing an entry, which is why deletions do
not come back.

Every record carries its own timestamp, and a record older than the entry
already held is skipped. That is redundant while replay order matches write
order — it stops being redundant once compaction rewrites old records into newly
created files, at which point file order no longer implies record age.

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

| policy | ops/sec | ns/op | a power loss can take |
|---|---:|---:|---|
| `SyncNever` | ~521,000 | 1,920 | writes since the last file seal that the kernel has not written back yet |
| `SyncEveryN(100)` | ~26,200 | 38,184 | at most the last 100 writes |
| `SyncAlways` | ~261 | 3,831,932 | nothing that has returned |
| `Get` (policy has no effect) | ~1,261,000 | 793 | — |

Durability costs about **2,000×** on this hardware.

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

### How these were measured

```sh
go test -bench=. -benchtime=10000x -run='^$' ./...
```

- Apple M5, 10 cores, macOS 26.6.2, Go 1.26.6, darwin/arm64
- Local NVMe SSD, APFS, via `b.TempDir()`
- 10,000 iterations per benchmark, fixed rather than time-based so the three
  policies are directly comparable
- 100-byte values, 6–10 byte keys, single goroutine, no competing I/O

Read these as a comparison between policies rather than as absolute throughput:

- **`Get` never touches the disk here.** Its 1,000-key working set is ~130 KB
  and stays in the page cache for the whole run, so 793 ns is a memory read. A
  dataset larger than RAM would pay real I/O per lookup.
- **`fsync` cost is a property of the storage.** On macOS Go issues
  `F_FULLFSYNC`, which waits for the drive to flush its own write cache. Linux's
  default `fsync` often returns earlier, so published numbers elsewhere are
  usually measuring a weaker guarantee.
- **`ns/op` is a mean.** Under `SyncEveryN(100)`, 99 writes are fast and the
  100th absorbs a full flush; the average hides a latency no single operation
  actually experiences. Percentiles are not measured yet.
- Single-threaded, against a database that starts empty each run.

## Testing

```sh
go test ./...
```
