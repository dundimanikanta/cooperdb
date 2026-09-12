package cooperdb

import (
	"testing"
	"time"
)

// TestKeyDirPutGet stores an entry and reads it back, field for field.
func TestKeyDirPutGet(t *testing.T) {
	k := NewKeyDir()

	want := Entry{
		FileID:    1,
		Offset:    31,
		Size:      29,
		Timestamp: time.Now().UnixNano(),
	}

	k.Put([]byte("user:1"), want)

	got, ok := k.Get([]byte("user:1"))
	if !ok {
		t.Fatalf("Get after Put: ok = false, want true")
	}

	if got.FileID != want.FileID {
		t.Errorf("FileID = %d, want %d", got.FileID, want.FileID)
	}

	if got.Offset != want.Offset {
		t.Errorf("Offset = %d, want %d", got.Offset, want.Offset)
	}

	if got.Size != want.Size {
		t.Errorf("Size = %d, want %d", got.Size, want.Size)
	}

	if got.Timestamp != want.Timestamp {
		t.Errorf("Timestamp = %d, want %d", got.Timestamp, want.Timestamp)
	}
}

// TestKeyDirGetMissing checks that an absent key reports ok = false rather than
// returning a zero Entry that reads like a real location at offset 0.
func TestKeyDirGetMissing(t *testing.T) {
	k := NewKeyDir()

	_, ok := k.Get([]byte("nope"))
	if ok {
		t.Errorf("Get on empty keydir: ok = true, want false")
	}
}

// TestKeyDirPutOverwrites is the update path: a second Put for the same key
// replaces the first, which is how a newer record supersedes an older one.
func TestKeyDirPutOverwrites(t *testing.T) {
	k := NewKeyDir()
	key := []byte("user:1")

	older := Entry{FileID: 1, Offset: 0, Size: 31, Timestamp: 100}
	newer := Entry{FileID: 1, Offset: 31, Size: 29, Timestamp: 200}

	k.Put(key, older)
	k.Put(key, newer)

	got, ok := k.Get(key)
	if !ok {
		t.Fatalf("Get after two Puts: ok = false, want true")
	}

	if got.Offset != newer.Offset {
		t.Errorf("Offset = %d, want %d (the newer entry)", got.Offset, newer.Offset)
	}

	// an overwrite replaces, it does not add
	if k.Len() != 1 {
		t.Errorf("Len = %d, want 1", k.Len())
	}
}

// TestKeyDirDelete checks that a deleted key stops resolving.
func TestKeyDirDelete(t *testing.T) {
	k := NewKeyDir()
	key := []byte("user:1")

	k.Put(key, Entry{FileID: 1, Offset: 0, Size: 31, Timestamp: 100})
	k.Delete(key)

	_, ok := k.Get(key)
	if ok {
		t.Errorf("Get after Delete: ok = true, want false")
	}
}

// TestKeyDirDeleteMissing deletes a key that was never there; the builtin is a
// no-op on an absent key, so this must not panic.
func TestKeyDirDeleteMissing(t *testing.T) {
	k := NewKeyDir()

	k.Delete([]byte("nope"))

	if k.Len() != 0 {
		t.Errorf("Len = %d, want 0", k.Len())
	}
}

// TestKeyDirLen tracks the count across puts and a delete.
func TestKeyDirLen(t *testing.T) {
	k := NewKeyDir()

	if k.Len() != 0 {
		t.Errorf("Len on a new keydir = %d, want 0", k.Len())
	}

	k.Put([]byte("user:1"), Entry{FileID: 1, Offset: 0, Size: 31, Timestamp: 100})
	k.Put([]byte("user:2"), Entry{FileID: 1, Offset: 31, Size: 29, Timestamp: 200})

	if k.Len() != 2 {
		t.Errorf("Len after two Puts = %d, want 2", k.Len())
	}

	k.Delete([]byte("user:1"))

	if k.Len() != 1 {
		t.Errorf("Len after Delete = %d, want 1", k.Len())
	}
}

// TestKeyDirKeyIsCopied mutates the caller's slice after Put. The map key is a
// string, so it holds a copy — the entry must still be reachable under the
// original bytes.
func TestKeyDirKeyIsCopied(t *testing.T) {
	k := NewKeyDir()

	key := []byte("user:1")
	k.Put(key, Entry{FileID: 1, Offset: 0, Size: 31, Timestamp: 100})

	// the caller reuses its buffer, as a read loop would
	copy(key, []byte("user:9"))

	_, ok := k.Get([]byte("user:1"))
	if !ok {
		t.Errorf("original key no longer resolves: the key was not copied")
	}
}
