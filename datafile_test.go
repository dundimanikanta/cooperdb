package cooperdb

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// TestAppendFirstOffsetIsZero checks the absolute offset of the first record,
// not just that offsets move. A bug that shifts every offset by a constant
// leaves the gaps between them correct, so only an absolute assertion sees it.
func TestAppendFirstOffsetIsZero(t *testing.T) {
	// t.TempDir gives a fresh directory per test, removed automatically after
	file, err := OpenDataFile(t.TempDir(), 45)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	offset, err := file.Append(r)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// the first record into an empty file starts at byte 0, absolutely
	if offset != 0 {
		t.Errorf("offset = %d, want %d", offset, 0)
	}
}

// TestAppendOffsetsAreAbsolute appends several records and checks where each
// one actually starts — 0, then RecordSize, then 2*RecordSize for records of
// equal size. Deliberately not written as "each offset is RecordSize past the
// last", which passes even when every value is wrong.
func TestAppendOffsetsAreAbsolute(t *testing.T) {
	file, err := OpenDataFile(t.TempDir(), 45)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice1"),
	}
	r2 := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:2"),
		Value:     []byte("alice2"),
	}
	r3 := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:3"),
		Value:     []byte("alice2"),
	}

	offset1, err1 := file.Append(r)
	if err1 != nil {
		t.Fatalf("Append r failed: %v", err1)
	}

	offset2, err2 := file.Append(r2)
	if err2 != nil {
		t.Fatalf("Append r2 failed: %v", err2)
	}

	offset3, err3 := file.Append(r3)
	if err3 != nil {
		t.Fatalf("Append r3 failed: %v", err3)
	}

	// each want is the total size of everything written before that record,
	// built from RecordSize alone — never from a returned offset, or this
	// becomes a test of the gaps rather than of the offsets themselves
	var want1 int64 = 0
	want2 := want1 + int64(RecordSize(len(r.Key), len(r.Value)))
	want3 := want2 + int64(RecordSize(len(r2.Key), len(r2.Value)))

	if offset1 != want1 {
		t.Errorf("offset1 = %d, want %d", offset1, want1)
	}

	if offset2 != want2 {
		t.Errorf("offset2 = %d, want %d", offset2, want2)
	}

	if offset3 != want3 {
		t.Errorf("offset3 = %d, want %d", offset3, want3)
	}
}

// TestReopenContinuesOffset is the test this session exists for. A data file
// reopened over existing records must resume at the end of them; starting at 0
// would hand the keydir offsets pointing into bytes that are already occupied.
func TestReopenContinuesOffset(t *testing.T) {
	// one directory held for the whole test: calling t.TempDir() a second time
	// would return a different, empty dir and the reopen would pass regardless
	dir := t.TempDir()
	const id = 45

	file, err := OpenDataFile(dir, id)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	_, err = file.Append(r)
	if err != nil {
		t.Fatalf("Append r failed: %v", err)
	}

	// closing drops the in-memory offset, so the reopen has to recover it from
	// the file on disk rather than from anything still held in the struct
	err = file.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// same dir, same id — this reopen is the thing under test
	reopened, err := OpenDataFile(dir, id)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	r2 := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:2"),
		Value:     []byte("bob"),
	}

	offset, err := reopened.Append(r2)
	if err != nil {
		t.Fatalf("Append r2 failed: %v", err)
	}

	// the second record belongs after the first; an offset of 0 here means the
	// reopen started writing over records that were already there
	want := int64(RecordSize(len(r.Key), len(r.Value)))
	if offset != want {
		t.Errorf("offset after reopen = %d, want %d", offset, want)
	}
}

// TestAppendedBytesDecode reads the file back through the codec — the first
// end-to-end check that what Append writes is what Decode can read.
func TestAppendedBytesDecode(t *testing.T) {
	file, err := OpenDataFile(t.TempDir(), 45)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	_, err = file.Append(r)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// not strictly needed — the page cache already makes the write visible to a
	// reader on this machine — but it exercises Sync and keeps the test honest
	// if it ever reads from a separate process
	err = file.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// this test is in package cooperdb, so it can reach the unexported handle;
	// os.File.Name reports the path the file was opened with
	raw, err := os.ReadFile(file.file.Name())
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	// Decode reads the record at the front and ignores anything after it
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Timestamp != r.Timestamp {
		t.Errorf("Timestamp = %d, want %d", decoded.Timestamp, r.Timestamp)
	}

	if !bytes.Equal(decoded.Key, r.Key) {
		t.Errorf("Key = %q, want %q", decoded.Key, r.Key)
	}

	if !bytes.Equal(decoded.Value, r.Value) {
		t.Errorf("Value = %q, want %q", decoded.Value, r.Value)
	}
}

// TestReadAtRoundTrip reads each record back by the offset its Append returned.
// Landing on the middle one is the point — not just the record at the front.
func TestReadAtRoundTrip(t *testing.T) {
	file, err := OpenDataFile(t.TempDir(), 45)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	// different lengths, so a wrong size can't accidentally work
	records := []*Record{
		{Timestamp: time.Now().UnixNano(), Key: []byte("user:1"), Value: []byte("alice")},
		{Timestamp: time.Now().UnixNano(), Key: []byte("user:22"), Value: []byte("bob")},
		{Timestamp: time.Now().UnixNano(), Key: []byte("user:333"), Value: []byte("carol-longer")},
	}

	// what the keydir would hold: where each record starts and how big it is
	offsets := make([]int64, len(records))
	sizes := make([]int, len(records))

	for i, r := range records {
		offset, err := file.Append(r)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}

		offsets[i] = offset
		sizes[i] = RecordSize(len(r.Key), len(r.Value))
	}

	// out of write order, so reading sequentially from the front can't pass
	for _, i := range []int{1, 2, 0} {
		want := records[i]

		got, err := file.ReadAt(offsets[i], sizes[i])
		if err != nil {
			t.Fatalf("ReadAt record %d failed: %v", i, err)
		}

		if got.Timestamp != want.Timestamp {
			t.Errorf("record %d: Timestamp = %d, want %d", i, got.Timestamp, want.Timestamp)
		}

		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("record %d: Key = %q, want %q", i, got.Key, want.Key)
		}

		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("record %d: Value = %q, want %q", i, got.Value, want.Value)
		}
	}
}

// TestReadAtPastEOF asks for a record beyond the end of the file: an error, not
// a panic. This is the shape the recovery scan hits on a torn final write.
func TestReadAtPastEOF(t *testing.T) {
	file, err := OpenDataFile(t.TempDir(), 45)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	_, err = file.Append(r)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// one whole record past the only record in the file
	size := RecordSize(len(r.Key), len(r.Value))
	pastEnd := int64(size)

	got, err := file.ReadAt(pastEnd, size)
	if err == nil {
		t.Fatalf("ReadAt past EOF returned %+v, want an error", got)
	}

	// either EOF flavour is correct here; a nil error is not
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
}
