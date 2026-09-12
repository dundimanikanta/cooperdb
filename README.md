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

Not yet: durability guarantees around `fsync`, rebuilding the keydir on restart,
compaction, and concurrent access. Until recovery lands, reopening a directory
that already holds records will not find them.

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

## Testing

```sh
go test ./...
```
