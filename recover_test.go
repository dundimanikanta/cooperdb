package cooperdb

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDataFileIDsOrdering creates files out of order and past the point where a
// text sort and a numeric one diverge — "10" sorts before "9" without padding.
func TestDataFileIDsOrdering(t *testing.T) {
	dir := t.TempDir()

	for _, id := range []uint32{9, 10, 2, 0} {
		df, err := OpenDataFile(dir, id, createIfMissing)
		if err != nil {
			t.Fatalf("OpenDataFile %d failed: %v", id, err)
		}

		err = df.Close()
		if err != nil {
			t.Fatalf("Close %d failed: %v", id, err)
		}
	}

	ids, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	want := []uint32{0, 2, 9, 10}

	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}

	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %d, want %d", i, ids[i], want[i])
		}
	}
}

// TestDataFileIDsIgnoresOtherFiles checks that something else living in the
// directory is skipped rather than failing the open.
func TestDataFileIDsIgnoresOtherFiles(t *testing.T) {
	dir := t.TempDir()

	df, err := OpenDataFile(dir, 0, createIfMissing)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	err = df.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// a wrong extension, and a right extension with a name that is not an id
	err = os.WriteFile(filepath.Join(dir, "000001.hint"), []byte("x"), 0644)
	if err != nil {
		t.Fatalf("writing the hint file failed: %v", err)
	}

	err = os.WriteFile(filepath.Join(dir, "backup.data"), []byte("x"), 0644)
	if err != nil {
		t.Fatalf("writing the backup file failed: %v", err)
	}

	ids, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(ids) != 1 || ids[0] != 0 {
		t.Errorf("ids = %v, want [0]", ids)
	}
}

// TestDataFileIDsEmptyDir checks that a directory with no data files is a new
// database rather than an error.
func TestDataFileIDsEmptyDir(t *testing.T) {
	ids, err := dataFileIDs(t.TempDir())
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(ids) != 0 {
		t.Errorf("ids = %v, want none", ids)
	}
}

// TestApplyRecordStoresLocation checks the common case: a record the keydir has
// not seen becomes an entry pointing at where it lives.
func TestApplyRecordStoresLocation(t *testing.T) {
	kd := NewKeyDir()

	r := &Record{Timestamp: 100, Key: []byte("user:1"), Value: []byte("alice")}
	applyRecord(kd, 7, 64, r)

	got, ok := kd.Get(r.Key)
	if !ok {
		t.Fatalf("key missing after applyRecord")
	}

	if got.FileID != 7 {
		t.Errorf("FileID = %d, want 7", got.FileID)
	}

	if got.Offset != 64 {
		t.Errorf("Offset = %d, want 64", got.Offset)
	}

	if got.Size != RecordSize(len(r.Key), len(r.Value)) {
		t.Errorf("Size = %d, want %d", got.Size, RecordSize(len(r.Key), len(r.Value)))
	}

	if got.Timestamp != r.Timestamp {
		t.Errorf("Timestamp = %d, want %d", got.Timestamp, r.Timestamp)
	}
}

// TestApplyRecordNewerWins is the update path during replay: a later record for
// the same key repoints the entry.
func TestApplyRecordNewerWins(t *testing.T) {
	kd := NewKeyDir()
	key := []byte("user:1")

	applyRecord(kd, 0, 0, &Record{Timestamp: 100, Key: key, Value: []byte("alice")})
	applyRecord(kd, 1, 0, &Record{Timestamp: 300, Key: key, Value: []byte("alice2")})

	got, ok := kd.Get(key)
	if !ok {
		t.Fatalf("key missing after applyRecord")
	}

	if got.FileID != 1 || got.Timestamp != 300 {
		t.Errorf("entry = {FileID %d, Timestamp %d}, want {1, 300}", got.FileID, got.Timestamp)
	}
}

// TestApplyRecordStaleIgnored checks the guard that makes replay order-proof.
// Nothing produces an out-of-order record today, but merge does in week 3, when
// old records get rewritten into newly created files.
func TestApplyRecordStaleIgnored(t *testing.T) {
	kd := NewKeyDir()
	key := []byte("user:1")

	applyRecord(kd, 1, 0, &Record{Timestamp: 300, Key: key, Value: []byte("current")})
	applyRecord(kd, 0, 99, &Record{Timestamp: 50, Key: key, Value: []byte("stale")})

	got, ok := kd.Get(key)
	if !ok {
		t.Fatalf("key missing after applyRecord")
	}

	if got.Timestamp != 300 || got.Offset != 0 {
		t.Errorf("entry = {Offset %d, Timestamp %d}, want {0, 300}", got.Offset, got.Timestamp)
	}
}

// TestApplyRecordEqualTimestampLaterWins pins the strict > in the staleness
// check. Two writes can land in the same nanosecond, and skipping the second
// one would be a lost write.
func TestApplyRecordEqualTimestampLaterWins(t *testing.T) {
	kd := NewKeyDir()
	key := []byte("user:1")

	applyRecord(kd, 0, 0, &Record{Timestamp: 300, Key: key, Value: []byte("first")})
	applyRecord(kd, 2, 7, &Record{Timestamp: 300, Key: key, Value: []byte("second")})

	got, ok := kd.Get(key)
	if !ok {
		t.Fatalf("key missing after applyRecord")
	}

	if got.FileID != 2 || got.Offset != 7 {
		t.Errorf("entry = {FileID %d, Offset %d}, want {2, 7}", got.FileID, got.Offset)
	}
}

// TestApplyRecordTombstoneRemoves checks that a replayed tombstone takes the key
// back out rather than storing an entry for it.
func TestApplyRecordTombstoneRemoves(t *testing.T) {
	kd := NewKeyDir()
	key := []byte("user:1")

	applyRecord(kd, 0, 0, &Record{Timestamp: 100, Key: key, Value: []byte("alice")})
	applyRecord(kd, 0, 32, &Record{Timestamp: 200, Flags: flagTombstone, Key: key})

	_, ok := kd.Get(key)
	if ok {
		t.Errorf("key still present after a tombstone")
	}

	if kd.Len() != 0 {
		t.Errorf("keydir Len = %d, want 0", kd.Len())
	}
}

// TestApplyRecordEmptyValueIsNotDelete is why the flags byte exists: a stored
// empty value must survive replay instead of being read as a deletion.
func TestApplyRecordEmptyValueIsNotDelete(t *testing.T) {
	kd := NewKeyDir()
	key := []byte("user:1")

	applyRecord(kd, 0, 0, &Record{Timestamp: 100, Key: key, Value: []byte{}})

	_, ok := kd.Get(key)
	if !ok {
		t.Errorf("a key stored with an empty value went missing")
	}
}

// TestApplyRecordTombstoneThenPut checks a key can come back: a tombstone is
// only the newest record until another one lands.
func TestApplyRecordTombstoneThenPut(t *testing.T) {
	kd := NewKeyDir()
	key := []byte("user:1")

	applyRecord(kd, 0, 0, &Record{Timestamp: 100, Key: key, Value: []byte("alice")})
	applyRecord(kd, 0, 32, &Record{Timestamp: 200, Flags: flagTombstone, Key: key})
	applyRecord(kd, 0, 59, &Record{Timestamp: 300, Key: key, Value: []byte("alice-again")})

	got, ok := kd.Get(key)
	if !ok {
		t.Fatalf("key missing after being rewritten")
	}

	if got.Offset != 59 {
		t.Errorf("Offset = %d, want 59", got.Offset)
	}
}

// TestLoadKeyDirAcrossFiles replays two files and checks the later one wins,
// which is the ordering the whole of recovery depends on.
func TestLoadKeyDirAcrossFiles(t *testing.T) {
	dir := t.TempDir()

	// file 0: two keys
	df0, err := OpenDataFile(dir, 0, createIfMissing)
	if err != nil {
		t.Fatalf("OpenDataFile 0 failed: %v", err)
	}

	_, err = df0.Append(&Record{Timestamp: 100, Key: []byte("user:1"), Value: []byte("alice")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	_, err = df0.Append(&Record{Timestamp: 200, Key: []byte("user:2"), Value: []byte("bob")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	err = df0.Close()
	if err != nil {
		t.Fatalf("Close 0 failed: %v", err)
	}

	// file 1: one key rewritten, the other deleted
	df1, err := OpenDataFile(dir, 1, createIfMissing)
	if err != nil {
		t.Fatalf("OpenDataFile 1 failed: %v", err)
	}

	newer, err := df1.Append(&Record{Timestamp: 300, Key: []byte("user:1"), Value: []byte("alice2")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	_, err = df1.Append(&Record{Timestamp: 400, Flags: flagTombstone, Key: []byte("user:2")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	err = df1.Close()
	if err != nil {
		t.Fatalf("Close 1 failed: %v", err)
	}

	kd, highestID, _, err := loadKeyDir(dir)
	if err != nil {
		t.Fatalf("loadKeyDir failed: %v", err)
	}

	if highestID != 1 {
		t.Errorf("highestID = %d, want 1", highestID)
	}

	if kd.Len() != 1 {
		t.Errorf("keydir Len = %d, want 1", kd.Len())
	}

	got, ok := kd.Get([]byte("user:1"))
	if !ok {
		t.Fatalf("user:1 missing after replay")
	}

	if got.FileID != 1 || got.Offset != newer {
		t.Errorf("user:1 = {FileID %d, Offset %d}, want {1, %d}", got.FileID, got.Offset, newer)
	}

	_, ok = kd.Get([]byte("user:2"))
	if ok {
		t.Errorf("user:2 survived its tombstone")
	}
}

// TestLoadKeyDirEmptyDir checks that recovery over nothing is a new database.
func TestLoadKeyDirEmptyDir(t *testing.T) {
	kd, highestID, _, err := loadKeyDir(t.TempDir())
	if err != nil {
		t.Fatalf("loadKeyDir failed: %v", err)
	}

	if kd == nil {
		t.Fatalf("keydir is nil")
	}

	if kd.Len() != 0 {
		t.Errorf("keydir Len = %d, want 0", kd.Len())
	}

	if highestID != 0 {
		t.Errorf("highestID = %d, want 0", highestID)
	}
}

// TestLoadKeyDirStopsAtTornTail truncates the last record, the shape a crash
// mid-write leaves. Recovery has to return the intact records rather than
// refusing to open the database at all.
func TestLoadKeyDirStopsAtTornTail(t *testing.T) {
	dir := t.TempDir()

	df, err := OpenDataFile(dir, 0, createIfMissing)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	_, err = df.Append(&Record{Timestamp: 100, Key: []byte("user:1"), Value: []byte("alice")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	_, err = df.Append(&Record{Timestamp: 200, Key: []byte("user:2"), Value: []byte("bob")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	err = df.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// chop the tail of the second record off
	path := filepath.Join(dir, "000000.data")

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	err = os.Truncate(path, fi.Size()-5)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	kd, _, _, err := loadKeyDir(dir)
	if err != nil {
		t.Fatalf("loadKeyDir on a torn tail failed: %v", err)
	}

	if kd.Len() != 1 {
		t.Errorf("keydir Len = %d, want 1 (the intact record)", kd.Len())
	}

	_, ok := kd.Get([]byte("user:1"))
	if !ok {
		t.Errorf("the record before the torn one was lost")
	}
}
