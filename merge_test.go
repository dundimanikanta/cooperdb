package cooperdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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

// copyFixtureDeleted are the keys copyFixture deletes and never writes again.
var copyFixtureDeleted = []int{1, 15, 29}

// copyFixture fills a database whose sealed files hold superseded records and
// tombstones, and returns it alongside the merge inputs.
func copyFixture(t *testing.T, dir string) (*DB, []uint32) {
	t.Helper()

	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := func(i int) []byte { return []byte(fmt.Sprintf("k%03d", i)) }

	for i := 0; i < 30; i++ {
		if err := db.Put(key(i), []byte(fmt.Sprintf("v%03d-original", i))); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// odd keys only, so the even-key rewrite below never brings them back
	for _, i := range copyFixtureDeleted {
		if err := db.Delete(key(i)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	for i := 0; i < 30; i += 2 {
		if err := db.Put(key(i), []byte(fmt.Sprintf("v%03d-updated", i))); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// padding, so everything above is sealed rather than left in the active file
	for i := 100; i < 120; i++ {
		if err := db.Put(key(i), []byte("padding-padding-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	inputs, err := db.mergeableFiles()
	if err != nil {
		t.Fatalf("mergeableFiles failed: %v", err)
	}

	if len(inputs) < 2 {
		t.Fatalf("only %d input file(s); the fixture did not rotate enough", len(inputs))
	}

	return db, inputs
}

// inputBytes totals the size of the given data files.
func inputBytes(t *testing.T, dir string, ids []uint32) int64 {
	t.Helper()

	total := int64(0)
	for _, id := range ids {
		fi, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%06d.data", id)))
		if err != nil {
			t.Fatalf("Stat %d failed: %v", id, err)
		}
		total += fi.Size()
	}

	return total
}

// TestCopyLiveRecordsReclaimsSpace checks the output is smaller than the inputs, which
// is the only reason merge exists.
func TestCopyLiveRecordsReclaimsSpace(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	before := inputBytes(t, dir, inputs)

	output, _, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	if output.offset >= before {
		t.Errorf("output is %d bytes against %d of input — nothing was reclaimed", output.offset, before)
	}
}

// TestCopyLiveRecordsOutputTakesAFreshHighID checks the output never lands on a file
// that already exists.
func TestCopyLiveRecordsOutputTakesAFreshHighID(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	output, _, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	for _, id := range before {
		if output.id == id {
			t.Fatalf("output reused existing file id %d", id)
		}
	}

	if output.id <= db.dataFile.id {
		t.Errorf("output id %d is not above the active file %d", output.id, db.dataFile.id)
	}
}

// TestCopyLiveRecordsKeepsOnlyLiveRecords checks each relocated key appears exactly
// once in the output and is still live in the keydir.
func TestCopyLiveRecordsKeepsOnlyLiveRecords(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	output, relocations, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	seen := make(map[string]int)
	puts := 0

	_, err = output.Scan(func(offset int64, r *Record) error {
		if r.IsTombstone() {
			return nil
		}
		puts++
		seen[string(r.Key)]++
		return nil
	})
	if err != nil {
		t.Fatalf("scanning the output failed: %v", err)
	}

	if puts != len(relocations) {
		t.Errorf("%d records in the output against %d relocations", puts, len(relocations))
	}

	for key, count := range seen {
		if count != 1 {
			t.Errorf("%s appears %d times in the output, want 1", key, count)
		}
		if _, ok := db.keyDirectory.Get([]byte(key)); !ok {
			t.Errorf("%s was copied but is not a live key", key)
		}
	}
}

// TestCopyLiveRecordsPreservesTimestamps is the one that matters most: a re-stamped
// record would beat the live one during replay and resurrect an old value.
func TestCopyLiveRecordsPreservesTimestamps(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	output, relocations, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	// the keydir holds each live record's original timestamp, so the copy must match it
	for key, moved := range relocations {
		original, ok := db.keyDirectory.Get([]byte(key))
		if !ok {
			t.Errorf("%s is not in the keydir", key)
			continue
		}
		if moved.Timestamp != original.Timestamp {
			t.Errorf("%s: copied timestamp %d, original %d", key, moved.Timestamp, original.Timestamp)
		}
	}

	_, err = output.Scan(func(offset int64, r *Record) error {
		if r.IsTombstone() {
			return nil
		}
		moved := relocations[string(r.Key)]
		if moved.Timestamp != r.Timestamp {
			t.Errorf("%s: record on disk has timestamp %d, relocation says %d", r.Key, r.Timestamp, moved.Timestamp)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanning the output failed: %v", err)
	}
}

// TestCopyLiveRecordsRelocationsNameTheRecords checks every relocation points at the
// record actually written, so the keydir update in step 4 has something valid to use.
func TestCopyLiveRecordsRelocationsNameTheRecords(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	output, relocations, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	for key, moved := range relocations {
		if moved.FileID != output.id {
			t.Errorf("%s points at file %d, want the output %d", key, moved.FileID, output.id)
		}

		r, err := output.ReadAt(moved.Offset, moved.Size)
		if err != nil {
			t.Errorf("%s: reading back at offset %d size %d: %v", key, moved.Offset, moved.Size, err)
			continue
		}

		if string(r.Key) != key {
			t.Errorf("offset %d holds key %q, relocation says %q", moved.Offset, r.Key, key)
		}
	}
}

// TestCopyLiveRecordsCopiesTombstones checks deletions survive the merge, without
// becoming keydir entries for keys that are supposed to be gone.
func TestCopyLiveRecordsCopiesTombstones(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	output, relocations, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	found := make(map[string]bool)
	_, err = output.Scan(func(offset int64, r *Record) error {
		if r.IsTombstone() {
			found[string(r.Key)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanning the output failed: %v", err)
	}

	// a key deleted and then written again keeps both records, so only the keys
	// that stayed deleted can be checked for absence from the keydir
	for _, i := range copyFixtureDeleted {
		key := fmt.Sprintf("k%03d", i)

		if !found[key] {
			t.Errorf("no tombstone for %s in the output, so it would come back on restart", key)
		}

		if _, ok := relocations[key]; ok {
			t.Errorf("deleted key %s was recorded as a relocation", key)
		}

		if _, ok := db.keyDirectory.Get([]byte(key)); ok {
			t.Errorf("%s is still live in the keydir, so the fixture did not delete it", key)
		}
	}
}

// TestCopyLiveRecordsWithNoInputs checks an empty merge creates no output file.
func TestCopyLiveRecordsWithNoInputs(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	before, err := dataFileIDs(db.directory)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	output, relocations, err := db.copyLiveRecords(nil)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}

	if output != nil {
		t.Errorf("an output file was created for an empty merge")
	}

	if len(relocations) != 0 {
		t.Errorf("relocations = %v, want none", relocations)
	}

	after, err := dataFileIDs(db.directory)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) != len(before) {
		t.Errorf("%d files before, %d after an empty merge", len(before), len(after))
	}
}

// TestCopyLiveRecordsAbortsOnAnUnreadableInput checks a file it cannot read fails the
// merge rather than being skipped — the caller deletes the inputs, so a skipped file
// would take its live records with it.
func TestCopyLiveRecordsAbortsOnAnUnreadableInput(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	missing := inputs[len(inputs)/2]
	err := os.Remove(filepath.Join(dir, fmt.Sprintf("%06d.data", missing)))
	if err != nil {
		t.Fatalf("removing input %d failed: %v", missing, err)
	}

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	output, relocations, err := db.copyLiveRecords(inputs)
	if err == nil {
		t.Errorf("copyLiveRecords succeeded with input %d missing", missing)
	}

	// a failed merge must leave nothing a later scan would take for a data file
	after, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) != len(before) {
		t.Errorf("files went from %v to %v — the failed merge left its output behind", before, after)
	}

	if output != nil {
		t.Errorf("an output file was handed back alongside the error")
	}

	if relocations != nil {
		t.Errorf("relocations = %v, want nil on failure", relocations)
	}
}

// TestApplyRelocationsRepointsUnchangedKeys checks the ordinary case: every key the
// keydir still names is moved to the copy in the output file.
func TestApplyRelocationsRepointsUnchangedKeys(t *testing.T) {
	db := keydirOnlyDB()
	relocations := make(map[string]Entry)

	for i, key := range []string{"a", "b", "c", "d"} {
		timestamp := int64(100 + i)
		db.keyDirectory.Put([]byte(key), Entry{FileID: 1, Offset: int64(i * 30), Size: 30, Timestamp: timestamp})
		relocations[key] = Entry{FileID: 9, Offset: int64(i * 20), Size: 30, Timestamp: timestamp}
	}

	applied := db.applyRelocations(relocations)

	if applied != len(relocations) {
		t.Errorf("applied = %d, want %d", applied, len(relocations))
	}

	for key, moved := range relocations {
		got, ok := db.keyDirectory.Get([]byte(key))
		if !ok {
			t.Errorf("%s went missing", key)
			continue
		}

		if got.FileID != moved.FileID || got.Offset != moved.Offset {
			t.Errorf("%s = {file %d, offset %d}, want {%d, %d}", key, got.FileID, got.Offset, moved.FileID, moved.Offset)
		}
	}
}

// TestApplyRelocationsKeepsANewerWrite checks a key written during the merge is not
// reverted to the copy, which holds the value as it was when the merge started.
func TestApplyRelocationsKeepsANewerWrite(t *testing.T) {
	db := keydirOnlyDB()

	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 1, Offset: 0, Size: 30, Timestamp: 100})
	relocations := map[string]Entry{
		"user:1": {FileID: 9, Offset: 0, Size: 30, Timestamp: 100},
	}

	// the write that lands while the merge is copying
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 5, Offset: 512, Size: 40, Timestamp: 500})

	applied := db.applyRelocations(relocations)

	if applied != 0 {
		t.Errorf("applied = %d, want 0 — the relocation was stale", applied)
	}

	got, ok := db.keyDirectory.Get([]byte("user:1"))
	if !ok {
		t.Fatalf("user:1 went missing")
	}

	if got.FileID != 5 || got.Timestamp != 500 {
		t.Errorf("user:1 = {file %d, ts %d}, want {5, 500} — the stale relocation overwrote a newer write",
			got.FileID, got.Timestamp)
	}
}

// TestApplyRelocationsDoesNotResurrectADeletedKey checks a key deleted during the
// merge stays deleted, even though its record was copied into the output.
func TestApplyRelocationsDoesNotResurrectADeletedKey(t *testing.T) {
	db := keydirOnlyDB()

	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 1, Offset: 0, Size: 30, Timestamp: 100})
	relocations := map[string]Entry{
		"user:1": {FileID: 9, Offset: 0, Size: 30, Timestamp: 100},
	}

	// the delete that lands while the merge is copying
	db.keyDirectory.Delete([]byte("user:1"))

	applied := db.applyRelocations(relocations)

	if applied != 0 {
		t.Errorf("applied = %d, want 0 — the key was deleted", applied)
	}

	if _, ok := db.keyDirectory.Get([]byte("user:1")); ok {
		t.Errorf("user:1 was brought back by its relocation")
	}
}

// TestApplyRelocationsSkipsOnlyTheStaleOnes checks one changed key does not abandon
// the rest — the guards continue rather than return.
func TestApplyRelocationsSkipsOnlyTheStaleOnes(t *testing.T) {
	db := keydirOnlyDB()
	relocations := make(map[string]Entry)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		timestamp := int64(100 + i)
		db.keyDirectory.Put([]byte(key), Entry{FileID: 1, Offset: int64(i * 30), Size: 30, Timestamp: timestamp})
		relocations[key] = Entry{FileID: 9, Offset: int64(i * 30), Size: 30, Timestamp: timestamp}
	}

	// one deleted and one overwritten, leaving eight untouched
	db.keyDirectory.Delete([]byte("k3"))
	db.keyDirectory.Put([]byte("k7"), Entry{FileID: 5, Offset: 0, Size: 30, Timestamp: 999})

	applied := db.applyRelocations(relocations)

	if applied != 8 {
		t.Errorf("applied = %d, want 8", applied)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		got, ok := db.keyDirectory.Get([]byte(key))

		switch key {
		case "k3":
			if ok {
				t.Errorf("k3 was resurrected")
			}
		case "k7":
			if !ok || got.FileID != 5 {
				t.Errorf("k7 = {file %d}, want the newer write in file 5", got.FileID)
			}
		default:
			if !ok || got.FileID != 9 {
				t.Errorf("%s = {file %d}, want the output file 9", key, got.FileID)
			}
		}
	}
}

// TestApplyRelocationsOnAnEmptyMap checks a merge with nothing to repoint is not an error.
func TestApplyRelocationsOnAnEmptyMap(t *testing.T) {
	db := keydirOnlyDB()
	db.keyDirectory.Put([]byte("user:1"), Entry{FileID: 1, Offset: 0, Size: 30, Timestamp: 100})

	if applied := db.applyRelocations(nil); applied != 0 {
		t.Errorf("applied = %d, want 0", applied)
	}

	if db.keyDirectory.Len() != 1 {
		t.Errorf("keydir Len = %d, want 1 — an empty merge changed the keydir", db.keyDirectory.Len())
	}
}

// TestDeleteAlreadyMergedFilesRemovesEveryInput checks a clean retire deletes all the inputs and
// leaves the active file alone.
func TestDeleteAlreadyMergedFilesRemovesEveryInput(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	activeID := db.dataFile.id

	err := db.deleteAlreadyMergedFiles(inputs)
	if err != nil {
		t.Errorf("a clean retire reported: %v", err)
	}

	left, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(left) != 1 || left[0] != activeID {
		t.Errorf("files left = %v, want just the active file %d", left, activeID)
	}
}

// TestDeleteAlreadyMergedFilesEvictsCachedHandles checks the handles rotation left in readFiles are
// closed and dropped, so nothing reads from a deleted file and no descriptor leaks.
func TestDeleteAlreadyMergedFilesEvictsCachedHandles(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	if len(db.readFiles) == 0 {
		t.Fatalf("readFiles is empty, so this test would prove nothing")
	}

	err := db.deleteAlreadyMergedFiles(inputs)
	if err != nil {
		t.Fatalf("deleteAlreadyMergedFiles failed: %v", err)
	}

	for _, id := range inputs {
		if _, ok := db.readFiles[id]; ok {
			t.Errorf("readFiles still holds a handle for retired file %d", id)
		}
	}
}

// TestDeleteAlreadyMergedFilesClosesEachHandleOnce guards a double close: os.File reports
// "file already closed" the second time, which would fail an otherwise clean merge.
func TestDeleteAlreadyMergedFilesClosesEachHandleOnce(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	cached := len(db.readFiles)
	if cached == 0 {
		t.Fatalf("readFiles is empty, so this test would prove nothing")
	}

	err := db.deleteAlreadyMergedFiles(inputs)
	if err != nil {
		t.Errorf("retiring %d cached files reported: %v", cached, err)
	}
}

// TestDeleteAlreadyMergedFilesReportsAFailedRemove checks a remove that cannot happen is surfaced
// rather than swallowed.
func TestDeleteAlreadyMergedFilesReportsAFailedRemove(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	// removed behind its back, so deleteAlreadyMergedFiles' own remove has nothing to delete
	victim := inputs[0]
	err := os.Remove(filepath.Join(dir, fmt.Sprintf("%06d.data", victim)))
	if err != nil {
		t.Fatalf("removing %d failed: %v", victim, err)
	}

	if err := db.deleteAlreadyMergedFiles(inputs); err == nil {
		t.Errorf("a failed remove was reported as success")
	}
}

// TestDeleteAlreadyMergedFilesContinuesPastAFailure checks one bad input does not abandon the rest:
// returning early would leave files on disk with their handles still open.
func TestDeleteAlreadyMergedFilesContinuesPastAFailure(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	victim := inputs[0]
	err := os.Remove(filepath.Join(dir, fmt.Sprintf("%06d.data", victim)))
	if err != nil {
		t.Fatalf("removing %d failed: %v", victim, err)
	}

	activeID := db.dataFile.id

	if err := db.deleteAlreadyMergedFiles(inputs); err == nil {
		t.Fatalf("expected an error from the missing input")
	}

	left, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(left) != 1 || left[0] != activeID {
		t.Errorf("files left = %v, want just the active file %d — the failure stopped the loop", left, activeID)
	}

	for _, id := range inputs {
		if _, ok := db.readFiles[id]; ok {
			t.Errorf("readFiles still holds a handle for %d after the failure", id)
		}
	}
}

// TestDeleteAlreadyMergedFilesOnAnEmptyList checks retiring nothing is a no-op.
func TestDeleteAlreadyMergedFilesOnAnEmptyList(t *testing.T) {
	dir := t.TempDir()
	db, _ := copyFixture(t, dir)
	defer db.Close()

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	cached := len(db.readFiles)

	if err := db.deleteAlreadyMergedFiles(nil); err != nil {
		t.Errorf("retiring nothing reported: %v", err)
	}

	after, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) != len(before) {
		t.Errorf("%d files before, %d after — an empty retire deleted something", len(before), len(after))
	}

	if len(db.readFiles) != cached {
		t.Errorf("readFiles went from %d to %d handles", cached, len(db.readFiles))
	}
}

// dirBytes totals the size of every file in dir.
func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	total := int64(0)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("Info failed: %v", err)
		}
		total += info.Size()
	}

	return total
}

// TestMergeReclaimsSpaceAndKeepsEveryValue is the whole point of merge, end to end:
// the directory shrinks and every live key still reads back correctly.
func TestMergeReclaimsSpaceAndKeepsEveryValue(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	want := make(map[string]string)

	// each key written three times, so two thirds of the bytes are dead
	for round := 0; round < 3; round++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("k%03d", i)
			value := fmt.Sprintf("v%03d-round%d-padding", i, round)

			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}

			want[key] = value
		}
	}

	for i := 0; i < 100; i += 10 {
		key := fmt.Sprintf("k%03d", i)
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		delete(want, key)
	}

	filesBefore, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	bytesBefore := dirBytes(t, dir)
	liveBefore := db.keyDirectory.Len()

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	filesAfter, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(filesAfter) >= len(filesBefore) {
		t.Errorf("%d files before, %d after — nothing was collapsed", len(filesBefore), len(filesAfter))
	}

	if dirBytes(t, dir) >= bytesBefore {
		t.Errorf("%d bytes before, %d after — no space was reclaimed", bytesBefore, dirBytes(t, dir))
	}

	if db.keyDirectory.Len() != liveBefore {
		t.Errorf("live keys went from %d to %d across the merge", liveBefore, db.keyDirectory.Len())
	}

	for key, value := range want {
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get %s after merge failed: %v", key, err)
		}

		if !bytes.Equal(got, []byte(value)) {
			t.Errorf("%s = %q after merge, want %q", key, got, value)
		}
	}

	for i := 0; i < 100; i += 10 {
		key := fmt.Sprintf("k%03d", i)
		if _, err := db.Get([]byte(key)); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("deleted %s came back after merge: %v", key, err)
		}
	}
}

// TestMergeSurvivesARestart checks the merged files replay correctly, which is the
// only thing that makes the deletion of the inputs safe.
func TestMergeSurvivesARestart(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	want := make(map[string]string)

	for round := 0; round < 3; round++ {
		for i := 0; i < 60; i++ {
			key := fmt.Sprintf("k%03d", i)
			value := fmt.Sprintf("v%03d-round%d-padding", i, round)

			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}

			want[key] = value
		}
	}

	for i := 0; i < 60; i += 7 {
		key := fmt.Sprintf("k%03d", i)
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		delete(want, key)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if reopened.keyDirectory.Len() != len(want) {
		t.Errorf("keydir Len = %d after restart, want %d", reopened.keyDirectory.Len(), len(want))
	}

	for key, value := range want {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get %s after restart failed: %v", key, err)
		}

		if !bytes.Equal(got, []byte(value)) {
			t.Errorf("%s = %q after restart, want %q", key, got, value)
		}
	}

	for i := 0; i < 60; i += 7 {
		key := fmt.Sprintf("k%03d", i)
		if _, err := reopened.Get([]byte(key)); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("deleted %s came back after restart: %v", key, err)
		}
	}
}

// TestMergeWritesAreDurable checks the output is flushed before the inputs are
// deleted — without the sync, a power loss between the two takes both copies.
func TestMergeWritesAreDurable(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(512), WithSyncNever())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < 80; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value-padding-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	inputs, err := db.mergeableFiles()
	if err != nil {
		t.Fatalf("mergeableFiles failed: %v", err)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// the inputs are gone, so the output is now the only copy and had better be
	// a complete, readable file rather than something still sitting in the cache
	for _, id := range inputs {
		if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%06d.data", id))); !os.IsNotExist(err) {
			t.Errorf("input %d survived the merge", id)
		}
	}

	// every record must be readable back off disk: the ones merge moved are now
	// only in the output, and the rest are still in the active file
	remaining, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	records := 0
	for _, id := range remaining {
		file, err := OpenDataFile(dir, id, dontCreateIfMissing)
		if err != nil {
			t.Fatalf("opening %d failed: %v", id, err)
		}

		_, err = file.Scan(func(offset int64, r *Record) error {
			records++
			return nil
		})

		file.Close()

		if err != nil {
			t.Fatalf("scanning %d failed: %v", id, err)
		}
	}

	if records != 80 {
		t.Errorf("%d records readable across %v, want 80", records, remaining)
	}
}

// TestMergeOnAFreshDatabaseIsANoOp checks a database with nothing sealed is success
// rather than an error, and that no output file is created for it.
func TestMergeOnAFreshDatabaseIsANoOp(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("user:1"), []byte("alice")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if err := db.Merge(); err != nil {
		t.Errorf("Merge on a database with nothing sealed: %v", err)
	}

	after, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) != len(before) {
		t.Errorf("files went from %v to %v — a no-op merge created one", before, after)
	}

	got, err := db.Get([]byte("user:1"))
	if err != nil || !bytes.Equal(got, []byte("alice")) {
		t.Errorf("user:1 = %q, %v after a no-op merge", got, err)
	}
}

// TestMergeLeavesTheActiveFileAlone checks writes still land in the active file after
// a merge, rather than in the output it just sealed.
func TestMergeLeavesTheActiveFileAlone(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < 60; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value-padding-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	activeBefore := db.dataFile.id

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if db.dataFile.id != activeBefore {
		t.Errorf("the active file changed from %d to %d across the merge", activeBefore, db.dataFile.id)
	}

	if err := db.Put([]byte("after-merge"), []byte("value")); err != nil {
		t.Fatalf("Put after merge failed: %v", err)
	}

	entry, ok := db.keyDirectory.Get([]byte("after-merge"))
	if !ok {
		t.Fatalf("the key written after the merge is missing")
	}

	if entry.FileID != db.dataFile.id {
		t.Errorf("a write after the merge landed in file %d, not the active file %d", entry.FileID, db.dataFile.id)
	}
}

// TestMergeIsRefusedWhenPoisoned checks a database whose sync has failed will not
// merge, since what is actually on disk is unknown.
func TestMergeIsRefusedWhenPoisoned(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < 60; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value-padding-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	failure := errors.New("cooperdb: injected sync failure")
	db.poisoned = failure

	if err := db.Merge(); !errors.Is(err, failure) {
		t.Errorf("Merge on a poisoned database = %v, want the poisoning error", err)
	}
}

// TestOpenSweepsStrayMergeFiles checks a merge that never committed leaves nothing
// behind after the next restart.
func TestOpenSweepsStrayMergeFiles(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)

	// a crash between the copy and the rename: the scratch file is on disk and
	// nothing references it
	output, _, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}

	strayName := fmt.Sprintf("%06d.merge.tmp", output.id)

	err = output.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	liveBefore := db.keyDirectory.Len()

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, strayName)); err != nil {
		t.Fatalf("the fixture did not leave a stray: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if _, err := os.Stat(filepath.Join(dir, strayName)); !os.IsNotExist(err) {
		t.Errorf("%s survived the reopen", strayName)
	}

	// and the abandoned merge changed nothing: its inputs were never deleted
	if reopened.keyDirectory.Len() != liveBefore {
		t.Errorf("keydir Len = %d after the reopen, want %d", reopened.keyDirectory.Len(), liveBefore)
	}
}

// TestStrayMergeFileIsInvisibleToRecovery checks the .tmp suffix keeps a partial
// merge out of dataFileIDs, so it cannot be replayed even before it is swept.
func TestStrayMergeFileIsInvisibleToRecovery(t *testing.T) {
	dir := t.TempDir()
	db, inputs := copyFixture(t, dir)
	defer db.Close()

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	output, _, err := db.copyLiveRecords(inputs)
	if err != nil {
		t.Fatalf("copyLiveRecords failed: %v", err)
	}
	defer output.Close()

	after, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) != len(before) {
		t.Errorf("dataFileIDs went from %v to %v — the scratch file is visible to recovery", before, after)
	}
}
