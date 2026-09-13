package cooperdb

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

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
