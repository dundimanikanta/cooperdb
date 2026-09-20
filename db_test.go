package cooperdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rotationFixture fills a database with count records under the given size
// threshold, and returns it alongside the key and value for index i.
func rotationFixture(t *testing.T, dir string, threshold int64, count int) *DB {
	t.Helper()

	db, err := Open(dir, WithMaxFileSize(threshold))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < count; i++ {
		err = db.Put(rotationKey(i), rotationValue(i))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	return db
}

func rotationKey(i int) []byte {
	return []byte(fmt.Sprintf("user:%d", i))
}

func rotationValue(i int) []byte {
	return []byte(fmt.Sprintf("value-%d-padding-padding", i))
}

// TestDBPutGet stores a value and reads it back — the whole engine end to end,
// through the keydir and out to the file.
func TestDBPutGet(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("user:1")
	value := []byte("alice")

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get = %q, want %q", got, value)
	}
}

// TestDBGetMissing checks that an unknown key is an error rather than a nil
// value, which the caller could not tell apart from a key stored empty.
func TestDBGetMissing(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	got, err := db.Get([]byte("nope"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v, want ErrKeyNotFound", err)
	}

	if got != nil {
		t.Errorf("value = %q, want nil", got)
	}
}

// TestDBPutOverwrites writes the same key twice. The newer record wins, and the
// older one is still on disk but no longer reachable.
func TestDBPutOverwrites(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("user:1")
	older := []byte("alice")
	newer := []byte("alice-updated")

	err = db.Put(key, older)
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	err = db.Put(key, newer)
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, newer) {
		t.Errorf("Get = %q, want %q", got, newer)
	}

	// an overwrite replaces the keydir entry rather than adding one
	if db.keyDirectory.Len() != 1 {
		t.Errorf("keydir Len = %d, want 1", db.keyDirectory.Len())
	}
}

// TestDBMultipleKeys interleaves several keys of different sizes and reads each
// back, so a wrong offset or size shows up as the wrong value.
func TestDBMultipleKeys(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	keys := [][]byte{
		[]byte("user:1"),
		[]byte("user:22"),
		[]byte("user:333"),
	}
	values := [][]byte{
		[]byte("alice"),
		[]byte("bob"),
		[]byte("carol-a-longer-value"),
	}

	for i := range keys {
		err = db.Put(keys[i], values[i])
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// read them out of write order, so reading sequentially cannot pass
	for _, i := range []int{1, 2, 0} {
		got, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Get %q failed: %v", keys[i], err)
		}

		if !bytes.Equal(got, values[i]) {
			t.Errorf("Get %q = %q, want %q", keys[i], got, values[i])
		}
	}

	if db.keyDirectory.Len() != len(keys) {
		t.Errorf("keydir Len = %d, want %d", db.keyDirectory.Len(), len(keys))
	}
}

// TestDBEmptyValue stores a zero-length value. It must come back as an empty
// value with no error — the state a tombstone will occupy in 2.3, and the
// reason a missing key cannot be reported as a nil value.
func TestDBEmptyValue(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("user:1")

	err = db.Put(key, []byte{})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("len(value) = %d, want 0", len(got))
	}
}

// TestDBPut100Keys is the bar for session 2.2: 100 keys written and all 100
// read back in the same process. Values are of varying length, so every record
// sits at an offset the previous ones determined — an error that drifts by even
// one byte compounds instead of cancelling out.
func TestDBPut100Keys(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const count = 100

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("user:%d", i))
		value := []byte(fmt.Sprintf("value-%d%s", i, strings.Repeat("x", i%7)))

		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if db.keyDirectory.Len() != count {
		t.Fatalf("keydir Len = %d, want %d", db.keyDirectory.Len(), count)
	}

	// backwards, so a read that only works sequentially cannot pass
	for i := count - 1; i >= 0; i-- {
		key := []byte(fmt.Sprintf("user:%d", i))
		want := []byte(fmt.Sprintf("value-%d%s", i, strings.Repeat("x", i%7)))

		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get %q failed: %v", key, err)
		}

		if !bytes.Equal(got, want) {
			t.Errorf("Get %q = %q, want %q", key, got, want)
		}
	}
}

// TestDBDelete stores a key, deletes it, and checks it stops resolving.
func TestDBDelete(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("user:1")

	err = db.Put(key, []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = db.Get(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrKeyNotFound", err)
	}

	if db.keyDirectory.Len() != 0 {
		t.Errorf("keydir Len = %d, want 0", db.keyDirectory.Len())
	}
}

// TestDBDeleteAppendsTombstone checks that the delete was written, not just
// applied in memory: the file has to grow, or a restart would resurrect the key.
func TestDBDeleteAppendsTombstone(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("user:1")

	err = db.Put(key, []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	afterPut := db.dataFile.offset

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// a tombstone is header + key, with no value
	want := afterPut + int64(RecordSize(len(key), 0))
	if db.dataFile.offset != want {
		t.Errorf("offset after Delete = %d, want %d", db.dataFile.offset, want)
	}
}

// TestDBDeleteMissing deletes a key that was never stored. It must not error.
func TestDBDeleteMissing(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Delete([]byte("nope"))
	if err != nil {
		t.Errorf("Delete of an absent key returned %v, want nil", err)
	}

	if db.keyDirectory.Len() != 0 {
		t.Errorf("keydir Len = %d, want 0", db.keyDirectory.Len())
	}
}

// TestDBDeleteThenPut brings a deleted key back. Nothing about a tombstone is
// permanent — it is just the newest record for that key until another one lands.
func TestDBDeleteThenPut(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("user:1")

	err = db.Put(key, []byte("alice"))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = db.Put(key, []byte("alice-again"))
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, []byte("alice-again")) {
		t.Errorf("Get = %q, want %q", got, "alice-again")
	}
}

// TestDBDeleteOneOfMany checks a delete touches only its own key.
func TestDBDeleteOneOfMany(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put user:1 failed: %v", err)
	}

	err = db.Put([]byte("user:2"), []byte("bob"))
	if err != nil {
		t.Fatalf("Put user:2 failed: %v", err)
	}

	err = db.Delete([]byte("user:1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	got, err := db.Get([]byte("user:2"))
	if err != nil {
		t.Fatalf("Get user:2 failed: %v", err)
	}

	if !bytes.Equal(got, []byte("bob")) {
		t.Errorf("Get user:2 = %q, want %q", got, "bob")
	}

	if db.keyDirectory.Len() != 1 {
		t.Errorf("keydir Len = %d, want 1", db.keyDirectory.Len())
	}
}

// TestDBPutAfterClose checks that Close actually closed the file: a write to a
// closed handle must come back as an error rather than being silently dropped.
func TestDBPutAfterClose(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err == nil {
		t.Errorf("Put after Close returned nil, want an error")
	}
}

// TestDBDefaultSyncPolicyIsNever checks that opening with no options leaves the
// policy at the zero value, so adding the option machinery changed no behaviour.
func TestDBDefaultSyncPolicyIsNever(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.syncPolicy != SyncNever {
		t.Errorf("syncPolicy = %d, want %d (SyncNever)", db.syncPolicy, SyncNever)
	}
}

// TestDBWithSyncAlways checks the option reaches the DB.
func TestDBWithSyncAlways(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncAlways())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.syncPolicy != SyncAlways {
		t.Errorf("syncPolicy = %d, want %d (SyncAlways)", db.syncPolicy, SyncAlways)
	}
}

// TestDBWithSyncEveryN checks the option sets both the policy and its interval,
// which is the reason the two travel together in one option.
func TestDBWithSyncEveryN(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncEveryN(100))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.syncPolicy != SyncEveryN {
		t.Errorf("syncPolicy = %d, want %d (SyncEveryN)", db.syncPolicy, SyncEveryN)
	}

	if db.syncEveryN != 100 {
		t.Errorf("syncEveryN = %d, want 100", db.syncEveryN)
	}
}

// TestDBWithSyncEveryNClampsBelowOne checks that an interval under 1 becomes 1.
// Left alone it would be a counter that never fires — durability silently turned
// off for someone who asked for it.
func TestDBWithSyncEveryNClampsBelowOne(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncEveryN(0))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.syncEveryN != 1 {
		t.Errorf("syncEveryN = %d, want 1", db.syncEveryN)
	}
}

// TestDBLastSyncOptionWins pins the behaviour of conflicting options, since
// Open applies them in order and nothing rejects a contradiction.
func TestDBLastSyncOptionWins(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncAlways(), WithSyncNever())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.syncPolicy != SyncNever {
		t.Errorf("syncPolicy = %d, want %d (SyncNever, the last option)", db.syncPolicy, SyncNever)
	}
}

// TestDBSyncEveryNResetsCounter follows writeCount across an interval boundary.
// Without the reset the counter would stay above the interval and flush on every
// write from then on, quietly turning SyncEveryN into SyncAlways.
func TestDBSyncEveryNResetsCounter(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncEveryN(3))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// two writes short of the interval
	for i := 0; i < 2; i++ {
		err = db.Put([]byte(fmt.Sprintf("user:%d", i)), []byte("alice"))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if db.writeCount != 2 {
		t.Errorf("writeCount after 2 writes = %d, want 2", db.writeCount)
	}

	// the third write reaches the interval and flushes
	err = db.Put([]byte("user:2"), []byte("alice"))
	if err != nil {
		t.Fatalf("third Put failed: %v", err)
	}

	if db.writeCount != 0 {
		t.Errorf("writeCount after the flush = %d, want 0", db.writeCount)
	}

	// and counting starts again rather than staying past the interval
	err = db.Put([]byte("user:3"), []byte("alice"))
	if err != nil {
		t.Fatalf("fourth Put failed: %v", err)
	}

	if db.writeCount != 1 {
		t.Errorf("writeCount after the next write = %d, want 1", db.writeCount)
	}
}

// TestDBWorksUnderEachSyncPolicy checks the policy changes only durability, not
// behaviour: the same writes and reads have to work under all three.
func TestDBWorksUnderEachSyncPolicy(t *testing.T) {
	policies := []struct {
		name string
		opt  Option
	}{
		{"SyncNever", WithSyncNever()},
		{"SyncAlways", WithSyncAlways()},
		{"SyncEveryN", WithSyncEveryN(2)},
	}

	for _, p := range policies {
		db, err := Open(t.TempDir(), p.opt)
		if err != nil {
			t.Fatalf("%s: Open failed: %v", p.name, err)
		}

		err = db.Put([]byte("user:1"), []byte("alice"))
		if err != nil {
			t.Fatalf("%s: Put failed: %v", p.name, err)
		}

		got, err := db.Get([]byte("user:1"))
		if err != nil {
			t.Fatalf("%s: Get failed: %v", p.name, err)
		}

		if !bytes.Equal(got, []byte("alice")) {
			t.Errorf("%s: Get = %q, want %q", p.name, got, "alice")
		}

		err = db.Delete([]byte("user:1"))
		if err != nil {
			t.Fatalf("%s: Delete failed: %v", p.name, err)
		}

		_, err = db.Get([]byte("user:1"))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("%s: Get after Delete = %v, want ErrKeyNotFound", p.name, err)
		}
	}
}

// TestDBPoisonedRefusesOperations is the point of the poisoning rule: once a
// sync has failed, what is on disk is unknown, so every later call is refused
// with the original failure rather than being served a maybe-stale answer.
func TestDBPoisonedRefusesOperations(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncAlways())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// closing the handle underneath is the cheapest way to make a real Sync fail
	err = db.dataFile.Close()
	if err != nil {
		t.Fatalf("closing the data file failed: %v", err)
	}

	syncErr := db.sync()
	if syncErr == nil {
		t.Fatalf("sync on a closed file returned nil, want an error")
	}

	if db.poisoned == nil {
		t.Fatalf("poisoned is nil after a failed sync")
	}

	// every entry point refuses, and reports the original cause
	err = db.Put([]byte("user:2"), []byte("bob"))
	if !errors.Is(err, syncErr) {
		t.Errorf("Put on a poisoned DB = %v, want %v", err, syncErr)
	}

	_, err = db.Get([]byte("user:1"))
	if !errors.Is(err, syncErr) {
		t.Errorf("Get on a poisoned DB = %v, want %v", err, syncErr)
	}

	err = db.Delete([]byte("user:1"))
	if !errors.Is(err, syncErr) {
		t.Errorf("Delete on a poisoned DB = %v, want %v", err, syncErr)
	}
}

// TestDBCloseOnPoisonedDB checks that shutdown is never refused. A database that
// cannot be closed is one whose file descriptor leaks.
func TestDBCloseOnPoisonedDB(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncAlways())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.dataFile.Close()
	if err != nil {
		t.Fatalf("closing the data file failed: %v", err)
	}

	if db.sync() == nil {
		t.Fatalf("sync on a closed file returned nil, want an error")
	}

	// Close still runs, and reports the sync failure rather than swallowing it
	err = db.Close()
	if err == nil {
		t.Errorf("Close on a poisoned DB returned nil, want the sync error")
	}
}

// TestDBReopenFindsAllKeys is the bar for session 2.5: 100 keys written, the
// process boundary crossed by closing and reopening, and all 100 read back.
func TestDBReopenFindsAllKeys(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const count = 100

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("user:%d", i))
		value := []byte(fmt.Sprintf("value-%d%s", i, strings.Repeat("x", i%7)))

		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// a second DB over the same directory, holding nothing the first one knew
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	if reopened.keyDirectory.Len() != count {
		t.Fatalf("keydir Len after reopen = %d, want %d", reopened.keyDirectory.Len(), count)
	}

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("user:%d", i))
		want := []byte(fmt.Sprintf("value-%d%s", i, strings.Repeat("x", i%7)))

		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get %q after reopen failed: %v", key, err)
		}

		if !bytes.Equal(got, want) {
			t.Errorf("Get %q = %q, want %q", key, got, want)
		}
	}
}

// TestDBReopenAfterOverwrite checks replay picks the newer of two records for
// the same key rather than whichever it happens to read first.
func TestDBReopenAfterOverwrite(t *testing.T) {
	dir := t.TempDir()
	key := []byte("user:1")

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put(key, []byte("alice"))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	err = db.Put(key, []byte("alice-updated"))
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}

	if !bytes.Equal(got, []byte("alice-updated")) {
		t.Errorf("Get = %q, want %q", got, "alice-updated")
	}

	// both records are still on disk; only one of them is reachable
	if reopened.keyDirectory.Len() != 1 {
		t.Errorf("keydir Len = %d, want 1", reopened.keyDirectory.Len())
	}
}

// TestDBReopenAfterDelete is the tombstone's first real job. Without it, replay
// would find the original record and hand back a key the caller deleted.
func TestDBReopenAfterDelete(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put user:1 failed: %v", err)
	}

	err = db.Put([]byte("user:2"), []byte("bob"))
	if err != nil {
		t.Fatalf("Put user:2 failed: %v", err)
	}

	err = db.Delete([]byte("user:1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	_, err = reopened.Get([]byte("user:1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("deleted key after reopen: err = %v, want ErrKeyNotFound", err)
	}

	got, err := reopened.Get([]byte("user:2"))
	if err != nil {
		t.Fatalf("Get user:2 after reopen failed: %v", err)
	}

	if !bytes.Equal(got, []byte("bob")) {
		t.Errorf("Get user:2 = %q, want %q", got, "bob")
	}
}

// TestDBReopenThenPutAppends checks recovery leaves the write offset at the end
// of the file. Resuming at 0 would write over records that are already there,
// and the damage would only surface on the restart after this one.
func TestDBReopenThenPutAppends(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("before"), []byte("written-first"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	err = reopened.Put([]byte("after"), []byte("written-second"))
	if err != nil {
		t.Fatalf("Put after reopen failed: %v", err)
	}

	// the new record must not have landed on top of the old one
	got, err := reopened.Get([]byte("before"))
	if err != nil {
		t.Fatalf("Get of the earlier key failed: %v", err)
	}

	if !bytes.Equal(got, []byte("written-first")) {
		t.Errorf("earlier key = %q, want %q", got, "written-first")
	}

	// and it has to survive a second trip through recovery
	err = reopened.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	third, err := Open(dir)
	if err != nil {
		t.Fatalf("second reopen failed: %v", err)
	}

	if third.keyDirectory.Len() != 2 {
		t.Errorf("keydir Len = %d, want 2", third.keyDirectory.Len())
	}
}

// TestDBOpenEmptyDirectory checks a directory with no data files opens as a new
// database rather than failing.
func TestDBOpenEmptyDirectory(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open on an empty directory failed: %v", err)
	}

	if db.keyDirectory.Len() != 0 {
		t.Errorf("keydir Len = %d, want 0", db.keyDirectory.Len())
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Errorf("Put on a new database failed: %v", err)
	}
}

// TestDBRotatesPastThreshold checks the active file is sealed before it grows past the limit.
func TestDBRotatesPastThreshold(t *testing.T) {
	dir := t.TempDir()

	db := rotationFixture(t, dir, 1024, 200)
	defer db.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	// five is the number session 2.6 asks for at a 1KB threshold
	if len(entries) < 5 {
		t.Fatalf("data files = %d, want at least 5", len(entries))
	}

	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("Info for %s failed: %v", e.Name(), err)
		}

		if info.Size() > 1024 {
			t.Errorf("%s is %d bytes, past the 1024 threshold", e.Name(), info.Size())
		}
	}
}

// TestDBReadsAcrossFiles is the point of the session: a key written before a rotation still reads.
func TestDBReadsAcrossFiles(t *testing.T) {
	const count = 200

	db := rotationFixture(t, t.TempDir(), 1024, count)

	// backwards, so the oldest keys — the ones in sealed files — are read first
	for i := count - 1; i >= 0; i-- {
		got, err := db.Get(rotationKey(i))
		if err != nil {
			t.Fatalf("Get %q failed: %v", rotationKey(i), err)
		}

		if !bytes.Equal(got, rotationValue(i)) {
			t.Errorf("Get %q = %q, want %q", rotationKey(i), got, rotationValue(i))
		}
	}
}

// TestDBEntriesSpanSeveralFiles checks the keydir records which file took each record.
func TestDBEntriesSpanSeveralFiles(t *testing.T) {
	const count = 200

	db := rotationFixture(t, t.TempDir(), 1024, count)

	seen := make(map[uint32]bool)

	for i := 0; i < count; i++ {
		entry, ok := db.keyDirectory.Get(rotationKey(i))
		if !ok {
			t.Fatalf("key %q missing from the keydir", rotationKey(i))
		}

		seen[entry.FileID] = true
	}

	if len(seen) < 2 {
		t.Errorf("entries reference %d distinct file ids, want more than one", len(seen))
	}
}

// TestDBOversizedRecordGetsItsOwnFile checks a record larger than the threshold is still stored.
func TestDBOversizedRecordGetsItsOwnFile(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(100))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("big")
	value := bytes.Repeat([]byte("x"), 500)

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Put of an oversized record failed: %v", err)
	}

	// a second write proves the first did not leave the database wedged
	err = db.Put([]byte("small"), []byte("v"))
	if err != nil {
		t.Fatalf("Put after an oversized record failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get of the oversized record failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("oversized value came back %d bytes, want %d", len(got), len(value))
	}
}

// TestDBRotationKeepsSealedFilesOpen checks a sealed file is cached rather than closed.
func TestDBRotationKeepsSealedFilesOpen(t *testing.T) {
	db := rotationFixture(t, t.TempDir(), 1024, 200)

	if len(db.readFiles) == 0 {
		t.Fatalf("no sealed files cached after rotating")
	}

	activeID := db.dataFile.id

	if _, ok := db.readFiles[activeID]; ok {
		t.Errorf("the active file id %d is also in readFiles", activeID)
	}
}

// TestDBReopenAcrossManyFiles checks recovery replays every file, not just the newest.
func TestDBReopenAcrossManyFiles(t *testing.T) {
	const count = 200

	dir := t.TempDir()

	db := rotationFixture(t, dir, 1024, count)

	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// without this the test would still pass if rotation had fired only once,
	// and recovery across many files would go unproven
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) < 5 {
		t.Fatalf("data files = %d, want at least 5 to replay across", len(entries))
	}

	reopened, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	if reopened.keyDirectory.Len() != count {
		t.Fatalf("keydir Len after reopen = %d, want %d", reopened.keyDirectory.Len(), count)
	}

	for i := 0; i < count; i++ {
		got, err := reopened.Get(rotationKey(i))
		if err != nil {
			t.Fatalf("Get %q after reopen failed: %v", rotationKey(i), err)
		}

		if !bytes.Equal(got, rotationValue(i)) {
			t.Errorf("Get %q = %q, want %q", rotationKey(i), got, rotationValue(i))
		}
	}
}

// TestDBCacheFillsLazilyAfterReopen checks a restart opens only the active file, then caches on demand.
func TestDBCacheFillsLazilyAfterReopen(t *testing.T) {
	const count = 200

	dir := t.TempDir()

	db := rotationFixture(t, dir, 1024, count)

	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	if len(reopened.readFiles) != 0 {
		t.Errorf("readFiles after reopen = %d, want 0", len(reopened.readFiles))
	}

	// key 0 lives in the very first file, so reading it must open exactly one
	_, err = reopened.Get(rotationKey(0))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(reopened.readFiles) != 1 {
		t.Fatalf("readFiles after one sealed read = %d, want 1", len(reopened.readFiles))
	}

	// a second key from the same file reuses the handle rather than opening another
	_, err = reopened.Get(rotationKey(1))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(reopened.readFiles) != 1 {
		t.Errorf("readFiles after a second read of the same file = %d, want 1", len(reopened.readFiles))
	}
}

// TestDBCloseReleasesCachedFiles checks sealed handles are closed too, not just the active one.
func TestDBCloseReleasesCachedFiles(t *testing.T) {
	db := rotationFixture(t, t.TempDir(), 1024, 200)

	cached := len(db.readFiles)
	if cached == 0 {
		t.Fatalf("no sealed files cached, so this test proves nothing")
	}

	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if len(db.readFiles) != 0 {
		t.Errorf("readFiles after Close = %d, want 0 (%d handles leaked)", len(db.readFiles), cached)
	}
}

// TestDBDeleteRotates checks a tombstone is a write like any other and can seal a file.
func TestDBDeleteRotates(t *testing.T) {
	dir := t.TempDir()

	// the put is 32 bytes and the tombstone 27, so 40 leaves room for one but
	// not both
	db, err := Open(dir, WithMaxFileSize(40))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	before := db.dataFile.id

	err = db.Delete([]byte("user:1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if db.dataFile.id == before {
		t.Errorf("active file id is still %d, want the tombstone to have rotated it", before)
	}

	_, err = db.Get([]byte("user:1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after Delete = %v, want ErrKeyNotFound", err)
	}
}

// TestDBMaxFileSizeClampsBelowOne checks a threshold under 1 falls back to the default.
func TestDBMaxFileSizeClampsBelowOne(t *testing.T) {
	db, err := Open(t.TempDir(), WithMaxFileSize(0))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.maxFileSize != defaultMaxFileSize {
		t.Errorf("maxFileSize = %d, want the default %d", db.maxFileSize, defaultMaxFileSize)
	}
}

// TestDBRotatesOnlyPastTheThreshold checks the boundary: a record that fits exactly does not rotate.
func TestDBRotatesOnlyPastTheThreshold(t *testing.T) {
	dir := t.TempDir()

	key := []byte("k")
	value := []byte("v")
	size := int64(RecordSize(len(key), len(value)))

	// room for exactly two records, so the third is the first that cannot fit
	db, err := Open(dir, WithMaxFileSize(2*size))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	if db.dataFile.id != 0 {
		t.Fatalf("rotated after one record, active id = %d", db.dataFile.id)
	}

	// same length as the first, so this fills the file to exactly the threshold
	err = db.Put([]byte("l"), value)
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	if db.dataFile.id != 0 {
		t.Errorf("rotated at exactly the threshold, active id = %d, want 0", db.dataFile.id)
	}

	err = db.Put([]byte("m"), value)
	if err != nil {
		t.Fatalf("third Put failed: %v", err)
	}

	if db.dataFile.id != 1 {
		t.Errorf("active id = %d, want 1 — the third record should not have fit", db.dataFile.id)
	}
}

// TestDBThresholdBelowRecordSize checks every record getting its own file still terminates.
func TestDBThresholdBelowRecordSize(t *testing.T) {
	dir := t.TempDir()

	const count = 5

	// 1 byte cannot hold even a header, so no record ever fits
	db, err := Open(dir, WithMaxFileSize(1))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < count; i++ {
		err = db.Put(rotationKey(i), rotationValue(i))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != count {
		t.Errorf("data files = %d, want %d — one per record", len(entries), count)
	}

	for i := 0; i < count; i++ {
		got, err := db.Get(rotationKey(i))
		if err != nil {
			t.Fatalf("Get %q failed: %v", rotationKey(i), err)
		}

		if !bytes.Equal(got, rotationValue(i)) {
			t.Errorf("Get %q = %q, want %q", rotationKey(i), got, rotationValue(i))
		}
	}
}

// TestDBOverwriteAcrossFilesSurvivesReopen checks replay prefers the newer record in a later file.
func TestDBOverwriteAcrossFilesSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	key := []byte("user:0")

	db := rotationFixture(t, dir, 1024, 200)

	// key:0 lives in the first file; this rewrite lands in the newest one
	err := db.Put(key, []byte("rewritten-much-later"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	oldEntry, _ := db.keyDirectory.Get(key)
	if oldEntry.FileID == 0 {
		t.Fatalf("the rewrite landed in file 0, so this test proves nothing")
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}

	if !bytes.Equal(got, []byte("rewritten-much-later")) {
		t.Errorf("Get = %q, want the rewrite — an earlier file won", got)
	}
}

// TestDBDeleteAcrossFilesSurvivesReopen checks a tombstone in a later file removes a key from an earlier one.
func TestDBDeleteAcrossFilesSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	deleted := []byte("user:0")

	db := rotationFixture(t, dir, 1024, 200)

	err := db.Delete(deleted)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	_, err = reopened.Get(deleted)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("deleted key came back after reopen: err = %v, want ErrKeyNotFound", err)
	}

	// the neighbour in the same sealed file must be untouched
	got, err := reopened.Get(rotationKey(1))
	if err != nil {
		t.Fatalf("Get of the neighbouring key failed: %v", err)
	}

	if !bytes.Equal(got, rotationValue(1)) {
		t.Errorf("neighbour = %q, want %q", got, rotationValue(1))
	}
}

// TestDBGetMissingDataFileErrors checks a dangling entry errors rather than creating an empty file.
func TestDBGetMissingDataFileErrors(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// an entry pointing at a file that was never written — the shape compaction
	// will produce in week 3 when it deletes a file a reader still points at
	db.keyDirectory.Put([]byte("ghost"), Entry{FileID: 99, Offset: 0, Size: 30, Timestamp: 1})

	_, err = db.Get([]byte("ghost"))
	if err == nil {
		t.Fatalf("Get of a dangling entry returned nil, want an error")
	}

	if !os.IsNotExist(err) {
		t.Errorf("err = %v, want a not-exist error", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	// dontCreateIfMissing is what keeps this at one file rather than two
	if len(entries) != 1 {
		t.Errorf("data files = %d, want 1 — the failed read created one", len(entries))
	}
}

// TestDBEmptyValueAcrossRotation checks a zero-length value is still not a tombstone after a reopen.
func TestDBEmptyValueAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	key := []byte("empty")

	db := rotationFixture(t, dir, 1024, 200)

	err := db.Put(key, []byte{})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir, WithMaxFileSize(1024))
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("Get of an empty value after reopen failed: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("len(value) = %d, want 0", len(got))
	}
}

// TestDBReopenWithDifferentThreshold checks the threshold is a runtime setting, not stored on disk.
func TestDBReopenWithDifferentThreshold(t *testing.T) {
	const count = 200

	dir := t.TempDir()

	db := rotationFixture(t, dir, 1024, count)

	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// reopened with a threshold the existing files already exceed
	reopened, err := Open(dir, WithMaxFileSize(64))
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	if reopened.keyDirectory.Len() != count {
		t.Errorf("keydir Len = %d, want %d", reopened.keyDirectory.Len(), count)
	}

	got, err := reopened.Get(rotationKey(0))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, rotationValue(0)) {
		t.Errorf("Get = %q, want %q", got, rotationValue(0))
	}
}

// TestDBTornTailDoesNotHideLaterWrites is the regression test for a two-restart
// data-loss bug: a crash leaves a partial record, the next Open appends past it
// instead of over it, and every record written after becomes invisible to the
// recovery scan after that — because records are found by chaining from the
// previous one, so one unreadable record breaks the chain permanently.
func TestDBTornTailDoesNotHideLaterWrites(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("before"), []byte("written-first"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// a crash mid-write leaves a fragment too short to be a record. Appended
	// directly, because killing the process would not produce one — the page
	// cache survives a SIGKILL and a record is a single Write call.
	path := filepath.Join(dir, "000000.data")

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, dataFilePerm)
	if err != nil {
		t.Fatalf("opening the file to damage it failed: %v", err)
	}

	_, err = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02})
	if err != nil {
		t.Fatalf("writing the fragment failed: %v", err)
	}

	err = f.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// dropped without Close, as a crash would leave it
	err = db.dataFile.file.Close()
	if err != nil {
		t.Fatalf("closing the handle failed: %v", err)
	}

	// first restart: recovery stops at the fragment, which is correct
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("first reopen failed: %v", err)
	}

	_, err = reopened.Get([]byte("before"))
	if err != nil {
		t.Fatalf("the record written before the damage was lost: %v", err)
	}

	err = reopened.Put([]byte("after"), []byte("written-second"))
	if err != nil {
		t.Fatalf("Put after reopening failed: %v", err)
	}

	err = reopened.dataFile.file.Close()
	if err != nil {
		t.Fatalf("closing the handle failed: %v", err)
	}

	// second restart: this is where the bug showed. The record written after
	// the damage must still be reachable.
	third, err := Open(dir)
	if err != nil {
		t.Fatalf("second reopen failed: %v", err)
	}

	if third.keyDirectory.Len() != 2 {
		t.Errorf("keydir Len = %d, want 2", third.keyDirectory.Len())
	}

	got, err := third.Get([]byte("after"))
	if err != nil {
		t.Fatalf("the record written after the damage was lost: %v", err)
	}

	if !bytes.Equal(got, []byte("written-second")) {
		t.Errorf("Get = %q, want %q", got, "written-second")
	}
}

// TestDBTimestampsNeverRepeat checks no two writes share a timestamp.
func TestDBTimestampsNeverRepeat(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const count = 20000

	seen := make(map[int64]bool, count)

	for i := 0; i < count; i++ {
		err = db.Put([]byte(fmt.Sprintf("user:%d", i)), []byte("alice"))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}

		if seen[db.lastTimestamp] {
			t.Fatalf("timestamp %d repeated at write %d", db.lastTimestamp, i)
		}

		seen[db.lastTimestamp] = true
	}
}

// TestDBTimestampResumesAfterRestart checks the counter is rebuilt from the log.
func TestDBTimestampResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	stored := db.lastTimestamp

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	if reopened.lastTimestamp != stored {
		t.Errorf("lastTimestamp after reopen = %d, want %d", reopened.lastTimestamp, stored)
	}
}

// TestDBTimestampAheadOfClockSurvivesRestart is why seeding exists: a burst can
// push the counter past the real clock, and a reset would then reuse a stored value.
func TestDBTimestampAheadOfClockSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// what a heavy burst does, without writing a million records
	db.lastTimestamp += int64(time.Second)

	err = db.Put([]byte("user:2"), []byte("bob"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	ahead := db.lastTimestamp

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening failed: %v", err)
	}

	err = reopened.Put([]byte("user:3"), []byte("carol"))
	if err != nil {
		t.Fatalf("Put after reopening failed: %v", err)
	}

	if reopened.lastTimestamp <= ahead {
		t.Errorf("post-restart timestamp = %d, want greater than the stored %d",
			reopened.lastTimestamp, ahead)
	}
}

// TestReplayOrderDoesNotChangeResult checks that unique timestamps make replay
// order irrelevant for puts — the property merge depends on.
func TestReplayOrderDoesNotChangeResult(t *testing.T) {
	records := []struct {
		fileID uint32
		offset int64
		r      *Record
	}{
		{0, 0, &Record{Timestamp: 100, Key: []byte("user:1"), Value: []byte("v1")}},
		{1, 0, &Record{Timestamp: 200, Key: []byte("user:2"), Value: []byte("other")}},
		{1, 30, &Record{Timestamp: 300, Key: []byte("user:1"), Value: []byte("v2")}},
		{3, 0, &Record{Timestamp: 400, Key: []byte("user:1"), Value: []byte("v3")}},
	}

	orders := [][]int{
		{0, 1, 2, 3},
		{3, 2, 1, 0},
		{2, 0, 3, 1},
		{3, 0, 2, 1},
	}

	for _, order := range orders {
		kd := NewKeyDir()

		for _, i := range order {
			applyRecord(kd, records[i].fileID, records[i].offset, records[i].r)
		}

		got, ok := kd.Get([]byte("user:1"))
		if !ok {
			t.Fatalf("order %v: user:1 missing", order)
		}

		if got.Timestamp != 400 || got.FileID != 3 {
			t.Errorf("order %v: entry = {FileID %d, Timestamp %d}, want {3, 400}",
				order, got.FileID, got.Timestamp)
		}

		if kd.Len() != 2 {
			t.Errorf("order %v: keydir Len = %d, want 2", order, kd.Len())
		}
	}
}

// TestTombstoneAppliedAfterItsPut checks the normal case: a delete replayed
// after the record it deletes.
func TestTombstoneAppliedAfterItsPut(t *testing.T) {
	kd := NewKeyDir()

	applyRecord(kd, 0, 0, &Record{Timestamp: 100, Key: []byte("user:1"), Value: []byte("alice")})
	applyRecord(kd, 1, 0, &Record{Timestamp: 200, Flags: flagTombstone, Key: []byte("user:1")})
	kd.dropTombstones()

	if _, ok := kd.Get([]byte("user:1")); ok {
		t.Errorf("key survived a newer tombstone")
	}
}

// TestTombstoneOrderingIsSymmetric is the reverse of TestTombstoneAppliedAfterItsPut:
// a tombstone replayed before the put it deletes must still win, since merge can
// leave an old record in a file numbered above the tombstone's.
func TestTombstoneOrderingIsSymmetric(t *testing.T) {
	kd := NewKeyDir()

	applyRecord(kd, 1, 0, &Record{Timestamp: 200, Flags: flagTombstone, Key: []byte("user:1")})
	applyRecord(kd, 0, 0, &Record{Timestamp: 100, Key: []byte("user:1"), Value: []byte("alice")})
	kd.dropTombstones()

	if _, ok := kd.Get([]byte("user:1")); ok {
		t.Errorf("deleted key resurrected by an older put replayed after it")
	}
}

// TestDeletedKeyAfterReopenIsNotFound checks a swept tombstone reads as a clean
// miss rather than as a decode failure from a zero-valued entry.
func TestDeletedKeyAfterReopenIsNotFound(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = db.Put([]byte("user:1"), []byte("alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = db.Delete([]byte("user:1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	_, err = db2.Get([]byte("user:1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after reopen = %v, want ErrKeyNotFound", err)
	}

	if db2.keyDirectory.Len() != 0 {
		t.Errorf("keydir Len = %d, want 0", db2.keyDirectory.Len())
	}
}

// TestNextFileIDNeverRepeats checks the allocator hands out a fresh id every time.
func TestNextFileIDNeverRepeats(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	seen := map[uint32]bool{db.dataFile.id: true}

	for i := 0; i < 10; i++ {
		id := db.nextFileID()
		if seen[id] {
			t.Fatalf("nextFileID handed out %d twice", id)
		}
		seen[id] = true
	}
}

// TestRotateSkipsAnAllocatedID checks rotation takes its id from the shared
// allocator, so a file merge has already reserved is not opened a second time.
func TestRotateSkipsAnAllocatedID(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(64))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	err = db.Put(rotationKey(0), rotationValue(0))
	if err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	// as Merge will: reserve an id without opening the file yet
	claimed := db.nextFileID()

	err = db.Put(rotationKey(1), rotationValue(1))
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	if db.dataFile.id == claimed {
		t.Errorf("rotation opened file %d, already reserved by another caller", claimed)
	}
}
