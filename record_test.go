package cooperdb

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// TestRoundTrip checks that a record survives Encode followed by Decode
// with its timestamp, key and value all intact.
func TestRoundTrip(t *testing.T) {

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	encoded := r.Encode()
	decoded, err := Decode(encoded)

	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Timestamp != r.Timestamp {
		t.Errorf("Timestamp = %d, want %d", decoded.Timestamp, r.Timestamp)

	}

	if !bytes.Equal(decoded.Key, r.Key) {
		t.Errorf("key = %q, want %q", decoded.Key, r.Key)

	}

	if !bytes.Equal(decoded.Value, r.Value) {
		t.Errorf("Value = %q, want %q", decoded.Value, r.Value)

	}
}

// TestTrailingBytes checks that Decode reads only its own record and ignores
// anything after it, which is what a scan over a log file will hand it.
func TestTrailingBytes(t *testing.T) {

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	encoded := r.Encode()
	withTrailing := append(encoded, []byte("the next record starts here")...)

	decoded, err := Decode(withTrailing)

	if err != nil {
		t.Fatalf("Decode failed on a buffer with trailing bytes: %v", err)
	}

	if !bytes.Equal(decoded.Key, r.Key) {
		t.Errorf("Key = %q, want %q", decoded.Key, r.Key)
	}

	if !bytes.Equal(decoded.Value, r.Value) {
		t.Errorf("Value = %q, want %q", decoded.Value, r.Value)
	}
}

// TestEmptyValue checks the shape a tombstone takes: a key that round-trips
// normally alongside a value of zero length.
func TestEmptyValue(t *testing.T) {

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte{},
	}

	encoded := r.Encode()
	decoded, err := Decode(encoded)

	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(decoded.Key, r.Key) {
		t.Errorf("Key = %q, want %q", decoded.Key, r.Key)
	}

	if len(decoded.Value) != 0 {
		t.Errorf("len(Value) = %d, want 0", len(decoded.Value))
	}
}

// TestCorruptedRecord flips a byte inside the key region and checks that the
// crc catches it rather than letting the damaged record decode successfully.
func TestCorruptedRecord(t *testing.T) {

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	encoded := r.Encode()
	encoded[25] ^= 0xFF

	_, err := Decode(encoded)

	if !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("err = %v, want ErrCorruptRecord", err)
	}
}

// TestShortBuffer checks that a buffer smaller than the header returns an
// error instead of panicking — the case recovery hits on a torn final write.
func TestShortBuffer(t *testing.T) {

	short := make([]byte, 5)

	_, err := Decode(short)

	if !errors.Is(err, ErrShortRecord) {
		t.Errorf("err = %v, want ErrShortRecord", err)
	}
}

// TestEncodedSize checks that Encode and RecordSize agree on a record's
// length, since the keydir relies on RecordSize to read the right byte count.
func TestEncodedSize(t *testing.T) {

	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("user:1"),
		Value:     []byte("alice"),
	}

	encoded := r.Encode()
	want := RecordSize(len(r.Key), len(r.Value))

	if len(encoded) != want {
		t.Errorf("len(encoded) = %d, want %d", len(encoded), want)
	}
}
