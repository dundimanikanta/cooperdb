package cooperdb

import (
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
