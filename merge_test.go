package cooperdb

import "testing"

// TestMergeableFilesExcludesTheActiveFile checks the file still being appended to
// is never offered up for merging.
func TestMergeableFilesExcludesTheActiveFile(t *testing.T) {
	dir := t.TempDir()

	db := rotationFixture(t, dir, 256, 40)
	defer db.Close()

	all, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(all) < 2 {
		t.Fatalf("only %d file(s) on disk, the fixture did not rotate", len(all))
	}

	ids, err := db.mergeableFiles()
	if err != nil {
		t.Fatalf("mergeableFiles failed: %v", err)
	}

	for _, id := range ids {
		if id == db.dataFile.id {
			t.Errorf("active file %d is in the mergeable set %v", id, ids)
		}
	}

	if len(ids) != len(all)-1 {
		t.Errorf("mergeable = %d files, want %d (every file but the active one)", len(ids), len(all)-1)
	}
}

// TestMergeableFilesKeepsCreationOrder checks the ids come back ascending, which is
// the order replay and merge both depend on.
func TestMergeableFilesKeepsCreationOrder(t *testing.T) {
	dir := t.TempDir()

	db := rotationFixture(t, dir, 256, 40)
	defer db.Close()

	ids, err := db.mergeableFiles()
	if err != nil {
		t.Fatalf("mergeableFiles failed: %v", err)
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids not ascending at index %d: %v", i, ids)
		}
	}
}

// TestMergeableFilesOnAFreshDatabase checks a database with only an active file has
// nothing to merge, and reports that as an empty slice rather than an error.
func TestMergeableFilesOnAFreshDatabase(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ids, err := db.mergeableFiles()
	if err != nil {
		t.Fatalf("mergeableFiles failed: %v", err)
	}

	if len(ids) != 0 {
		t.Errorf("mergeable = %v, want none", ids)
	}
}

// TestMergeableFilesAfterReopen checks the set is still correct when the active file
// is a fresh one and every older file is sealed.
func TestMergeableFilesAfterReopen(t *testing.T) {
	dir := t.TempDir()

	db := rotationFixture(t, dir, 256, 40)
	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	ids, err := reopened.mergeableFiles()
	if err != nil {
		t.Fatalf("mergeableFiles failed: %v", err)
	}

	all, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(ids) != len(all)-1 {
		t.Errorf("mergeable = %d files, want %d", len(ids), len(all)-1)
	}

	for _, id := range ids {
		if id == reopened.dataFile.id {
			t.Errorf("active file %d is in the mergeable set %v", id, ids)
		}
	}
}

// keydirOnlyDB builds a DB with nothing but a keydir, which is all isLive touches.
func keydirOnlyDB() *DB {
	return &DB{keyDirectory: NewKeyDir()}
}

// recordFor is a plain record for key; only its key is ever compared.
func recordFor(key string) *Record {
	return &Record{Timestamp: 100, Key: []byte(key), Value: []byte("value")}
}

// TestIsLiveMatchesTheRecordTheKeydirPointsAt is the one case that answers true.
func TestIsLiveMatchesTheRecordTheKeydirPointsAt(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 3, Offset: 64})

	if !db.isLive(3, 64, recordFor("user:1")) {
		t.Errorf("the record the keydir points at read as dead")
	}
}

// TestIsLiveRejectsSupersededRecords checks an older copy loses whether the newer one
// landed in another file or further down the same one.
func TestIsLiveRejectsSupersededRecords(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 3, Offset: 64})

	if db.isLive(1, 64, recordFor("user:1")) {
		t.Errorf("a copy in an older file read as live")
	}

	if db.isLive(3, 0, recordFor("user:1")) {
		t.Errorf("a copy at an earlier offset in the same file read as live")
	}
}

// TestIsLiveComparesBothFileAndOffset guards the De Morgan slip: an || that should be
// && rejects only records where both fields differ, so a matching offset in the wrong
// file would read as live.
func TestIsLiveComparesBothFileAndOffset(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 7, Offset: 64})

	if db.isLive(3, 64, recordFor("user:1")) {
		t.Errorf("matched on offset alone, ignoring the file id")
	}

	if db.isLive(7, 128, recordFor("user:1")) {
		t.Errorf("matched on file id alone, ignoring the offset")
	}
}

// TestIsLiveRejectsDeletedAndUnknownKeys checks both ways a key can be missing from
// the keydir.
func TestIsLiveRejectsDeletedAndUnknownKeys(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 3, Offset: 64})
	db.keyDirectory.Delete([]byte("user:1"))

	if db.isLive(3, 64, recordFor("user:1")) {
		t.Errorf("a deleted key's record read as live")
	}

	if db.isLive(0, 0, recordFor("never-written")) {
		t.Errorf("a key the keydir has never seen read as live")
	}
}

// TestIsLiveRejectsTombstoneRecords checks a tombstone answers false — merge copies it
// by policy, not because isLive calls it live.
func TestIsLiveRejectsTombstoneRecords(t *testing.T) {
	db := keydirOnlyDB()
	tombstone := &Record{Timestamp: 200, Flags: flagTombstone, Key: []byte("user:1")}

	if db.isLive(2, 10, tombstone) {
		t.Errorf("a tombstone read as live")
	}
}

// TestIsLiveRejectsATombstoneEntry guards the zero-value collision: a replay-only entry
// has FileID 0 and Offset 0, which names a real location — the first record of file 0.
func TestIsLiveRejectsATombstoneEntry(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{Timestamp: 500, Tombstone: true})

	if db.isLive(0, 0, recordFor("user:1")) {
		t.Errorf("a tombstone entry at (0,0) kept a record alive")
	}
}

// TestIsLiveMatchesOneCopyPerKey checks exactly one of several copies in a file is live.
func TestIsLiveMatchesOneCopyPerKey(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 5, Offset: 200})

	live := 0
	for _, offset := range []int64{0, 100, 200, 300} {
		if db.isLive(5, offset, recordFor("user:1")) {
			live++
		}
	}

	if live != 1 {
		t.Errorf("%d of 4 copies read as live, want 1", live)
	}
}
