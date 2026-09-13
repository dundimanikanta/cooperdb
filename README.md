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

Not yet: file rotation, compaction, and concurrent access.

## Recovery

The keydir lives only in memory. `Open` reconstructs it by replaying every data
file, which is what makes a write survive the process that made it.

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
| `SyncNever` | ~521,000 | 1,920 | every write the kernel has not written back yet |
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
