package cooperdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

// These tests are only meaningful under -race. The detector is dynamic: it
// reports races it actually observes, so a suite with no concurrent test passes
// while proving nothing. Run them with:
//
//	go test -race ./...

// concurrencyKey is the key for index i, shared by the goroutines below.
func concurrencyKey(i int) []byte {
	return []byte(fmt.Sprintf("k%03d", i))
}

// TestConcurrentReadsAndWrites is the shape that failed before the keydir was
// locked: readers and writers on the same keys at the same time.
func TestConcurrentReadsAndWrites(t *testing.T) {
	const keys = 50

	db, err := Open(t.TempDir(), WithMaxFileSize(2048))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < keys; i++ {
		if err := db.Put(concurrencyKey(i), []byte("value-original-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	var wg sync.WaitGroup

	// four readers, hitting every key repeatedly
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 300; i++ {
				value, err := db.Get(concurrencyKey(i % keys))
				if err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}

				// a torn read would show up here rather than as a race report
				if !bytes.HasPrefix(value, []byte("value-")) {
					t.Errorf("read a malformed value: %q", value)
					return
				}
			}
		}()
	}

	// two writers, overwriting the same keys the readers are reading
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()

			for i := 0; i < 300; i++ {
				err := db.Put(concurrencyKey(i%keys), []byte(fmt.Sprintf("value-w%d-padding", w)))
				if err != nil {
					t.Errorf("Put failed: %v", err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	if db.keyDirectory.Len() != keys {
		t.Errorf("keydir Len = %d, want %d", db.keyDirectory.Len(), keys)
	}
}

// TestConcurrentReadsAcrossFiles forces rotation while reads are in flight, so
// readers resolve files through fileFor while rotate is writing readFiles.
func TestConcurrentReadsAcrossFiles(t *testing.T) {
	const keys = 200

	dir := t.TempDir()

	// a small threshold, so the writer below rotates constantly
	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < keys; i++ {
		if err := db.Put(concurrencyKey(i), []byte("value-original-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	ids, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(ids) < 3 {
		t.Fatalf("only %d files; the fixture did not rotate enough to exercise fileFor", len(ids))
	}

	var wg sync.WaitGroup

	// readers reaching into older files, which fileFor has to open and cache
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < keys; i++ {
				if _, err := db.Get(concurrencyKey(i)); err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
			}
		}()
	}

	// one writer, rotating as it goes
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < keys; i++ {
			if err := db.Put(concurrencyKey(i), []byte("value-updated-padding")); err != nil {
				t.Errorf("Put failed: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

// TestConcurrentDeletesAndReads checks a reader hitting a key while it is being
// deleted gets either the value or ErrKeyNotFound, never a corrupt read.
func TestConcurrentDeletesAndReads(t *testing.T) {
	const keys = 50

	db, err := Open(t.TempDir(), WithMaxFileSize(2048))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < keys; i++ {
		if err := db.Put(concurrencyKey(i), []byte("value-original-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	var wg sync.WaitGroup

	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < keys; i++ {
				value, err := db.Get(concurrencyKey(i))

				// both outcomes are correct; anything else is not
				if err == ErrKeyNotFound {
					continue
				}
				if err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
				if !bytes.HasPrefix(value, []byte("value-")) {
					t.Errorf("read a malformed value: %q", value)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < keys; i++ {
			if err := db.Delete(concurrencyKey(i)); err != nil {
				t.Errorf("Delete failed: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	if db.keyDirectory.Len() != 0 {
		t.Errorf("keydir Len = %d after deleting every key, want 0", db.keyDirectory.Len())
	}
}

// TestConcurrentColdCacheMisses is the case mu cannot cover: two readers both
// hold mu for reading, which does not exclude them from each other, and both
// write readFiles when they miss. Only readFilesMu prevents that.
//
// Every goroutine starts at a different point in the keyspace against a freshly
// reopened database, so the misses happen simultaneously rather than the first
// reader warming the cache for the rest.
func TestConcurrentColdCacheMisses(t *testing.T) {
	const keys = 200

	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < keys; i++ {
		if err := db.Put(concurrencyKey(i), []byte("value-padding-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// reopened, so readFiles starts empty and every read is a miss
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if len(reopened.readFiles) != 0 {
		t.Fatalf("readFiles holds %d handles after a reopen, so the cache is not cold", len(reopened.readFiles))
	}

	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			for i := 0; i < keys; i++ {
				// a different starting point per goroutine, so they miss on
				// different files at the same moment
				if _, err := reopened.Get(concurrencyKey((g*25 + i) % keys)); err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

// TestConcurrentMergeWithReads is the hard case the merge design calls out: a
// read arriving for a record in a file merge is about to delete.
func TestConcurrentMergeWithReads(t *testing.T) {
	const keys = 200
	const rounds = 4

	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// rounds over the same keys, so the sealed files hold superseded records
	for round := 0; round < rounds; round++ {
		for i := 0; i < keys; i++ {
			value := []byte(fmt.Sprintf("value-r%d-padding", round))
			if err := db.Put(concurrencyKey(i), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(before) < 10 {
		t.Fatalf("only %d files; the fixture gives merge too little to do", len(before))
	}

	var wg sync.WaitGroup

	// closed when the merges are done, so the readers and writers below run for
	// the whole merge rather than finishing in its first millisecond
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)

		for m := 0; m < 8; m++ {
			if err := db.Merge(); err != nil {
				t.Errorf("Merge failed: %v", err)
				return
			}
		}
	}()

	// no key is ever deleted, so ErrKeyNotFound is never a legal answer here
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()

			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}

				key := concurrencyKey((r*31 + i) % keys)

				value, err := db.Get(key)
				if err != nil {
					t.Errorf("Get(%s) failed: %v", key, err)
					return
				}

				if !bytes.HasPrefix(value, []byte("value-")) {
					t.Errorf("read a malformed value: %q", value)
					return
				}
			}
		}(r)
	}

	// a disjoint key range, so the fixture's keys stay live and merge has real
	// records to relocate. Writing the same keys supersedes every one of them,
	// isLive then rejects the lot, and the merges become no-ops.
	//
	// Capped as well as signalled: unbounded writers would bury merge in files.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()

			for i := 0; i < keys*25; i++ {
				select {
				case <-done:
					return
				default:
				}

				value := []byte(fmt.Sprintf("value-w%d-padding", w))
				if err := db.Put(concurrencyKey(keys+i%keys), value); err != nil {
					t.Errorf("Put failed: %v", err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	// the fixture's keys plus the writers' disjoint range
	if db.keyDirectory.Len() != keys*2 {
		t.Errorf("keydir Len = %d, want %d", db.keyDirectory.Len(), keys*2)
	}

	for i := 0; i < keys*2; i++ {
		if _, err := db.Get(concurrencyKey(i)); err != nil {
			t.Errorf("Get(%s) failed after the merges: %v", concurrencyKey(i), err)
		}
	}

	// once the writers have stopped, so the count is not a moving target
	if err := db.Merge(); err != nil {
		t.Fatalf("final Merge failed: %v", err)
	}

	after, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) >= len(before) {
		t.Errorf("files went %d -> %d; the merges reclaimed nothing", len(before), len(after))
	}

	t.Logf("files %d -> %d, read retries %d", len(before), len(after), db.readRetries.Load())
}

// TestGetRetriesOnAClosedFile drives the retry branch directly, because the
// concurrent test above never opens the window it exists for.
func TestGetRetriesOnAClosedFile(t *testing.T) {
	const keys = 100

	db, err := Open(t.TempDir(), WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for i := 0; i < keys; i++ {
		if err := db.Put(concurrencyKey(i), []byte("value-original-padding")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// key 0 is in the first sealed file, so reading it caches that handle
	if _, err := db.Get(concurrencyKey(0)); err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	entry, ok := db.keyDirectory.Get(concurrencyKey(0))
	if !ok {
		t.Fatal("key 0 is not in the keydir")
	}

	cached, ok := db.readFiles[entry.FileID]
	if !ok {
		t.Fatalf("file %d is not cached, so the fixture read the active file", entry.FileID)
	}

	// what deleteAlreadyMergedFiles does, minus the eviction: every attempt then
	// finds the same closed handle, so the retries run out instead of succeeding
	if err := cached.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	before := db.readRetries.Load()

	_, err = db.Get(concurrencyKey(0))
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Get error = %v, want one wrapping os.ErrClosed", err)
	}

	if got := db.readRetries.Load() - before; got != 3 {
		t.Errorf("readRetries rose by %d, want 3", got)
	}
}

// TestConcurrentMergesLeaveNoOrphan is the regression guard for a measured bug: two
// merges at once each wrote an output, and only one of them was ever deleted.
//
// The timestamp CAS cannot catch it. Both merges copy the same live records, so both
// carry identical timestamps and the second repoint passes the comparison.
func TestConcurrentMergesLeaveNoOrphan(t *testing.T) {
	const keys = 150
	const rounds = 3

	dir := t.TempDir()

	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	for round := 0; round < rounds; round++ {
		for i := 0; i < keys; i++ {
			if err := db.Put(concurrencyKey(i), []byte("value-original-padding")); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}

	before, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(before) < 10 {
		t.Fatalf("only %d files; the fixture gives merge too little to do", len(before))
	}

	var wg sync.WaitGroup

	// a barrier, so both goroutines are inside Merge at the same moment rather than
	// one finishing before the other starts
	start := make(chan struct{})
	errs := make([]error, 2)

	for m := 0; m < 2; m++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			<-start
			errs[m] = db.Merge()
		}(m)
	}

	close(start)
	wg.Wait()

	merged := 0
	refused := 0

	for _, err := range errs {
		switch {
		case err == nil:
			merged++
		case errors.Is(err, ErrMergeInProgress):
			refused++
		default:
			t.Errorf("Merge returned an unexpected error: %v", err)
		}
	}

	if merged != 1 || refused != 1 {
		t.Errorf("%d merged and %d refused, want exactly 1 of each: %v", merged, refused, errs)
	}

	// the merged output plus the active file, and nothing orphaned between them
	after, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	if len(after) != 2 {
		t.Errorf("files went %d -> %d, want 2; a second output was left behind", len(before), len(after))
	}

	if db.keyDirectory.Len() != keys {
		t.Errorf("keydir Len = %d, want %d", db.keyDirectory.Len(), keys)
	}

	for i := 0; i < keys; i++ {
		if _, err := db.Get(concurrencyKey(i)); err != nil {
			t.Errorf("Get(%s) failed: %v", concurrencyKey(i), err)
		}
	}
}
