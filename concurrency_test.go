package cooperdb

import (
	"bytes"
	"fmt"
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
