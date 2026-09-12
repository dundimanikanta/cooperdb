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
