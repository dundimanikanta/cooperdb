package cooperdb

import (
	"errors"
	"time"
)

// ErrKeyNotFound is returned by Get for a key the keydir has no entry for.
var ErrKeyNotFound = errors.New("cooperdb: key not found")

// DB is the public handle: a keydir in memory over one or more data files on
// disk. Every write appends to the active file and updates the keydir.
type DB struct {
	// held for rotation (2.6), which needs somewhere to create the next file
	directory string

	// the file every write is appended to
	dataFile *DataFile

	// where each live key's newest record can be found
	keyDirectory *KeyDir
}

// Open prepares the database in dir for use.
func Open(dir string) (*DB, error) {
	// file id 0 for now: rotation is 2.6
	dataf, err := OpenDataFile(dir, 0)
	if err != nil {
		return nil, err
	}

	// starts empty — rebuilding it from files already on disk is 2.5, so until
	// then reopening a directory with existing records will not find them
	keyd := NewKeyDir()

	return &DB{
		directory:    dir,
		dataFile:     dataf,
		keyDirectory: keyd,
	}, nil
}

// Put stores value under key, appending a new record and repointing the keydir.
func (db *DB) Put(key, value []byte) error {
	r := &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}

	// a failed append must not reach the keydir, or the entry points at a
	// record that was never written
	offset, err := db.dataFile.Append(r)
	if err != nil {
		return err
	}

	// r.Timestamp, not a second time.Now(): the keydir has to carry the same
	// value the record on disk does, or 2.5's newest-wins compares the wrong one
	entry := Entry{
		FileID:    db.dataFile.id,
		Offset:    offset,
		Size:      RecordSize(len(key), len(value)),
		Timestamp: r.Timestamp,
	}

	db.keyDirectory.Put(key, entry)

	return nil
}

// Get returns the value most recently stored under key.
func (db *DB) Get(key []byte) ([]byte, error) {
	// a miss is an error, not a nil value: an empty value is a real state, so
	// the caller could not otherwise tell "absent" from "stored but empty"
	entry, ok := db.keyDirectory.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// the offset and size the keydir recorded when this record was written
	r, err := db.dataFile.ReadAt(entry.Offset, entry.Size)
	if err != nil {
		return nil, err
	}

	return r.Value, nil
}

// Close closes the active data file.
func (db *DB) Close() error {
	// whether a Sync belongs here first is the durability policy question in 2.4
	return db.dataFile.Close()
}

// Delete removes key. The record cannot be erased from an append-only file, so
// a tombstone is appended instead; the keydir drop is what makes the key
// unreachable now, and the tombstone is what stops recovery resurrecting it.
func (db *DB) Delete(key []byte) error {
	tombstone := &Record{
		Timestamp: time.Now().UnixNano(),
		// bit 0 set — this is what marks the deletion, not the empty value,
		// since Put(key, []byte{}) writes an empty value too
		Flags: flagTombstone,
		Key:   key,
		// nothing to store: len(nil) is 0, so this record is header + key only
		Value: nil,
	}

	// keydir last: dropping the key before a failed append would leave it gone
	// in memory but still live on disk, and the next restart would bring it back
	_, err := db.dataFile.Append(tombstone)
	if err != nil {
		return err
	}

	db.keyDirectory.Delete(key)

	return nil
}
