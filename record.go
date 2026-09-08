package cooperdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// headerSize is the fixed prefix on every record:
//
//	crc (4) + timestamp (8) + keySize (4) + valueSize (4)
const headerSize = 20

var (
	ErrCorruptRecord = errors.New("cooperdb: crc mismatch")
	ErrShortRecord   = errors.New("cooperdb: record shorter than header")
)

// Record is one entry as it exists in memory.
type Record struct {
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Encode lays r out as: header | key | value
//
// Layout:
//
//	[0:4]   crc32 over bytes [4:] of the finished record
//	[4:12]  timestamp
//	[12:16] len(Key)
//	[16:20] len(Value)
//	[20:20+keySize]              key bytes
//	[20+keySize:20+keySize+valSize] value bytes
func (r *Record) Encode() []byte {

	//allocating a slice of length headerSize + len(r.Key) + len(r.Value)
	buf := make([]byte, headerSize+len(r.Key)+len(r.Value))

	//writing timestamp into [4:12]
	//writing key size into [12:16]
	//writing value size into [16:20]
	binary.LittleEndian.PutUint64(buf[4:12], uint64(r.Timestamp))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(r.Key)))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(r.Value)))

	// copying r.Key in starting at offset 20
	// copying r.Value in starting at offset 20+len(r.Key)
	copy(buf[headerSize:], r.Key)
	copy(buf[headerSize+len(r.Key):], r.Value)

	// computing the  crc32.ChecksumIEEE over everything from offset 4 onward,
	// then writing it into [0:4]
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	// returning the the slice
	return buf
}

// Decode parses a record from the front of b, verifying the checksum.
// Any bytes after the record are ignored, so b may be a larger read buffer.
// Returns ErrShortRecord if b is too small, ErrCorruptRecord if the crc fails.
func Decode(b []byte) (*Record, error) {
	// need a full header before any of the size fields can be read
	if len(b) < headerSize {
		return nil, ErrShortRecord
	}

	// reading timestamp from [4:12], key size from [12:16], value size from [16:20]
	timestamp := binary.LittleEndian.Uint64(b[4:12])
	keySize := binary.LittleEndian.Uint32(b[12:16])
	valueSize := binary.LittleEndian.Uint32(b[16:20])

	// The sizes are still unverified here — the crc can't be checked until they
	// tell us where the record ends. Summing as uint64 keeps two near-max sizes
	// from overflowing int on a 32-bit platform, and the bounds check is what
	// makes it safe to slice with them below.
	total := uint64(headerSize) + uint64(keySize) + uint64(valueSize)
	if uint64(len(b)) < total {
		return nil, ErrShortRecord
	}
	end := int(total)

	// checksumming exactly this record, not the whole buffer, so trailing
	// bytes from a larger read don't break an otherwise valid record
	if crc32.ChecksumIEEE(b[4:end]) != binary.LittleEndian.Uint32(b[0:4]) {
		return nil, ErrCorruptRecord
	}

	// cloning because these slices alias b, which the caller may reuse
	keyEnd := headerSize + int(keySize)
	key := bytes.Clone(b[headerSize:keyEnd])
	value := bytes.Clone(b[keyEnd:end])

	return &Record{
		Timestamp: int64(timestamp),
		Key:       key,
		Value:     value,
	}, nil
}

// RecordSize reports the on-disk size of a record with the given key and value
// lengths. Useful later for advancing the read offset while scanning a file.
func RecordSize(keyLen, valueLen int) int {
	return headerSize + keyLen + valueLen
}
