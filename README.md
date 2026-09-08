# cooperdb

A log-structured key/value storage engine written in Go, implementing the
[Bitcask](https://riak.com/assets/bitcask-intro.pdf) design.

A cooper is the craftsman who makes casks — a nod to Bitcask, which this
follows closely. Work in progress; the record encoding layer is done.

Every write appends to the end of a log file rather than updating in place, so
writes stay sequential. An in-memory map holds the file and byte offset of each
key's newest record, which keeps a read to one map lookup plus one seek. The
tradeoff: every key lives in RAM, so the keyspace is bounded by memory.

## Record format

All integers little-endian.

| Offset | Size | Field |
|--------|------|-------|
| `0`  | 4 | CRC-32 (IEEE), over every byte from offset 4 to the end of the record |
| `4`  | 8 | timestamp, `int64` Unix nanoseconds |
| `12` | 4 | key size, `uint32` |
| `16` | 4 | value size, `uint32` |
| `20` | *key size* | key |
| `20 + key size` | *value size* | value (zero length marks a tombstone) |

The 20-byte header is fixed, so a reader reads the header first, learns the two
sizes, then reads exactly the rest of the record.

## Testing

```sh
go test ./...
```
