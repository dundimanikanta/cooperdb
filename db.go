package cooperdb

import (
	"errors"
	"time"
)

// ErrKeyNotFound is returned by Get for a key the keydir has no entry for.
var ErrKeyNotFound = errors.New("cooperdb: key not found")

type SyncPolicy int

const (
	SyncNever SyncPolicy = iota // default: the kernel decides
	SyncAlways
	SyncEveryN
)

// Option configures a DB at Open time. Each one is applied to the DB after it
// is built, so the zero values in the struct are the defaults.
type Option func(*DB)

// WithSyncNever leaves flushing entirely to the kernel. Fastest, and a write
// that has returned is lost if the machine loses power before the kernel
// writes it back. This is the default.
func WithSyncNever() Option {
	return func(db *DB) {
		db.syncPolicy = SyncNever
	}
}

// WithSyncAlways flushes on every write, so a write that has returned has
// reached the disk. Slowest by a wide margin — each write waits on hardware.
func WithSyncAlways() Option {
	return func(db *DB) {
		db.syncPolicy = SyncAlways
	}
}

// WithSyncEveryN flushes once every n writes, bounding what a power loss can
// take to the last n writes. n below 1 is treated as 1, since a zero or
// negative interval would otherwise mean a counter that never fires — silently
// turning this into WithSyncNever, which is the one mistake worth ruling out.
func WithSyncEveryN(n int) Option {
	if n < 1 {
		n = 1
	}

	return func(db *DB) {
		db.syncPolicy = SyncEveryN
		db.syncEveryN = n
	}
}

// DB is the public handle: a keydir in memory over one or more data files on
// disk. Every write appends to the active file and updates the keydir.
type DB struct {
	// held for rotation (2.6), which needs somewhere to create the next file
	directory string

	// the file every write is appended to
	dataFile *DataFile

	// where each live key's newest record can be found
	keyDirectory *KeyDir

	// when to flush; the zero value is SyncNever
	syncPolicy SyncPolicy

	// the flush interval, only meaningful under SyncEveryN
	syncEveryN int

	// carrying the writes count from last sync used only when under SyncEveryN
	writeCount int

	// defining the poisoned to store when error is retuned during the sync
	poisoned error
}

// Open prepares the database in dir for use. Called with no options it syncs
// never, that being the zero value of SyncPolicy.
func Open(dir string, opts ...Option) (*DB, error) {
	// file id 0 for now: rotation is 2.6
	dataf, err := OpenDataFile(dir, 0)
	if err != nil {
		return nil, err
	}

	// starts empty — rebuilding it from files already on disk is 2.5, so until
	// then reopening a directory with existing records will not find them
	keyd := NewKeyDir()

	db := &DB{
		directory:    dir,
		dataFile:     dataf,
		keyDirectory: keyd,
	}

	// applied after the struct is built, so an option always overwrites a real
	// default; passing two that conflict means the last one wins
	for _, opt := range opts {
		opt(db)
	}

	return db, nil
}

// syncAsPerPolicy flushes the data file if the configured policy calls for it.
// A failed sync poisons the DB permanently: it is never retried, because a
// failed fsync can leave the kernel treating the pages as clean, so a second
// call would report success over data that is already gone.
func (db *DB) syncAsPerPolicy() error {
	switch db.syncPolicy {

	// the kernel writes back on its own schedule; nothing to do here
	case SyncNever:
		return nil

	// every write waits on the hardware before returning
	case SyncAlways:
		return db.sync()

	// count this write, and flush only once the interval is reached
	case SyncEveryN:
		db.writeCount++

		// interval reached: reset the count first, so a failed sync cannot
		// leave it stuck past the interval and flushing on every write after
		if db.writeCount >= db.syncEveryN {
			db.writeCount = 0
			return db.sync()
		}

		// still short of the interval, so nothing is flushed this time
		return nil
	}

	return nil
}

// sync flushes the data file and records any failure as permanent.
// Every path that flushes goes through here, so there is exactly one place
// the database can become poisoned.
func (db *DB) sync() error {
	err := db.dataFile.Sync()

	// hold the original error: later calls report the real cause, not that
	// some unnamed failure happened at some point
	if err != nil {
		db.poisoned = err
		return err
	}

	return nil
}

// Put stores value under key, appending a new record and repointing the keydir.
func (db *DB) Put(key, value []byte) error {
	// a sync has failed, so what is actually on disk is unknown
	if db.poisoned != nil {
		return db.poisoned
	}

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

	// syncing the df as per the policy
	return db.syncAsPerPolicy()
}

// Get returns the value most recently stored under key.
func (db *DB) Get(key []byte) ([]byte, error) {
	// reads are refused too: serving a value would assert the database is in a
	// known state, which after a failed sync it is not
	if db.poisoned != nil {
		return nil, db.poisoned
	}

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

// Close flushes the active data file and closes it. The flush happens whatever
// the policy says: SyncNever means the kernel decides while the process runs,
// not that a clean shutdown is allowed to lose writes.
//
// There is deliberately no poisoned check — a database that cannot be closed is
// a database whose descriptor leaks, so shutting down always has to be allowed.
func (db *DB) Close() error {
	syncErr := db.sync()

	// closed even when the flush failed, or the descriptor leaks on exactly the
	// path where something has already gone wrong
	closeErr := db.dataFile.Close()

	// the sync error wins: it means data may be lost, where a close error
	// usually means only that the handle was already gone
	if syncErr != nil {
		return syncErr
	}

	return closeErr
}

// Delete removes key. The record cannot be erased from an append-only file, so
// a tombstone is appended instead; the keydir drop is what makes the key
// unreachable now, and the tombstone is what stops recovery resurrecting it.
func (db *DB) Delete(key []byte) error {
	// a delete is a write, so it is refused for the same reason a Put is
	if db.poisoned != nil {
		return db.poisoned
	}

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

	// syncing the df as per the policy
	return db.syncAsPerPolicy()
}
