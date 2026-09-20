package cooperdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

const defaultMaxFileSize = 2 << 30 // 2 GiB, a round number that is not too small

// Option configures a DB at Open time. Each one is applied to the DB after it
// is built, so the zero values in the struct are the defaults.
type Option func(*DB)

// WithSyncNever leaves flushing to the kernel: fastest, loses unflushed writes on power loss. The default.
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

// WithSyncEveryN flushes every n writes, so a power loss takes at most the last n.
// n below 1 clamps to 1: a counter that never fires would silently mean SyncNever.
func WithSyncEveryN(n int) Option {
	if n < 1 {
		n = 1
	}

	return func(db *DB) {
		db.syncPolicy = SyncEveryN
		db.syncEveryN = n
	}
}

// WithMaxFileSize seals the active file once it passes size bytes. A value below
// 1 falls back to the default, since a threshold of zero would seal every record.
func WithMaxFileSize(size int64) Option {

	if size < 1 {
		size = defaultMaxFileSize
	}
	return func(db *DB) {
		db.maxFileSize = size
	}
}

// DB is the public handle: a keydir in memory over one or more data files on
// disk. Every write appends to the active file and updates the keydir.
type DB struct {
	directory     string               // where data files live; rotation (2.6) creates the next one here
	dataFile      *DataFile            // the file every write is appended to
	keyDirectory  *KeyDir              // where each live key's newest record can be found
	syncPolicy    SyncPolicy           // when to flush; the zero value is SyncNever
	syncEveryN    int                  // the flush interval, only meaningful under SyncEveryN
	writeCount    int                  // writes since the last flush, only used under SyncEveryN
	poisoned      error                // set once a sync fails, and never cleared
	readFiles     map[uint32]*DataFile // open handles for reading old files, keyed by id
	maxFileSize   int64                // the size past which the active file is sealed and the next one started
	lastTimestamp int64                // the timestamp of the last written record
	lastFileID    uint32               // the id of the most recently allocated data file
}

// Open prepares the database in dir for use. Called with no options it syncs
// never, that being the zero value of SyncPolicy.
func Open(dir string, opts ...Option) (*DB, error) {
	// replay whatever is already on disk; an empty directory gives back an
	// empty keydir and id 0, which is a new database rather than an error
	keyd, activeID, lastValidLen, previousMaxTimestamp, err := loadKeyDir(dir)
	if err != nil {
		return nil, err
	}

	// 0 means the directory held no data files, so there is nothing to trim
	if activeID > 0 {
		// housekeeping now, not the chain-break fix it was: nothing appends to
		// that file again, so this only keeps its size equal to its valid length
		err = truncateToValid(dir, activeID-1, lastValidLen)
		if err != nil {
			return nil, err
		}

		// nothing readable in it, so no keydir entry points into it — take it
		// over rather than leaving an empty file behind on every restart
		if lastValidLen == 0 {
			activeID--
		}
	}

	// never the highest one on disk: merge output takes a high id, and appending
	// into a file merge owns would interleave records
	dataf, err := OpenDataFile(dir, activeID, createIfMissing)
	if err != nil {
		return nil, err
	}

	db := &DB{
		directory:     dir,
		dataFile:      dataf,
		keyDirectory:  keyd,
		maxFileSize:   defaultMaxFileSize,
		readFiles:     make(map[uint32]*DataFile),
		lastTimestamp: previousMaxTimestamp,
		lastFileID:    activeID,
	}

	// applied after the struct is built, so an option always overwrites a real
	// default; passing two that conflict means the last one wins
	for _, opt := range opts {
		opt(db)
	}

	return db, nil
}

// truncateToValid cuts a data file back to the length recovery could actually
// read, discarding a partial record left by a crash. The bytes removed are by
// definition unparseable — Decode already refused them.
func truncateToValid(dir string, id uint32, validLen int64) error {
	path := filepath.Join(dir, fmt.Sprintf("%06d.data", id))

	fi, err := os.Stat(path)

	// a new database: there is no file to trim
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	// the normal case, where nothing was torn
	if fi.Size() <= validLen {
		return nil
	}

	return os.Truncate(path, validLen)
}

// syncAsPerPolicy flushes the data file if the configured policy calls for it.
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

// sync flushes the data file and records any failure as permanent — the one
// place the database can become poisoned. Never retried: a failed fsync can
// leave the kernel marking the pages clean, so a second call would report
// success over data that is already gone.
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

func (db *DB) nextTimestamp() int64 {
	ts := time.Now().UnixNano()
	if ts <= db.lastTimestamp {
		ts = db.lastTimestamp + 1
	}
	db.lastTimestamp = ts
	return ts
}

// nextFileID reserves a file id nothing has used yet. Rotation and merge both
// allocate, so each computing its own successor would land them on one file.
func (db *DB) nextFileID() uint32 {
	db.lastFileID++
	return db.lastFileID
}

// Put stores value under key, appending a new record and repointing the keydir.
func (db *DB) Put(key, value []byte) error {
	// a sync has failed, so what is actually on disk is unknown
	if db.poisoned != nil {
		return db.poisoned
	}

	r := &Record{
		Timestamp: db.nextTimestamp(),
		Key:       key,
		Value:     value,
	}

	err := db.rotateIfFull(RecordSize(len(key), len(value)))

	if err != nil {
		return err
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

	keysFileID := entry.FileID

	df, err := db.fileFor(keysFileID)
	if err != nil {
		return nil, err
	}

	// the offset and size the keydir recorded when this record was written
	r, err := df.ReadAt(entry.Offset, entry.Size)
	if err != nil {
		return nil, err
	}

	return r.Value, nil
}

// fileFor resolves the file id in a keydir entry to an open file, opening and
// caching sealed files the first time a read reaches for one.
func (db *DB) fileFor(keysFileID uint32) (*DataFile, error) {

	// the active file is held separately, since it is the only writable one
	currentDataFileID := db.dataFile.id

	if keysFileID == currentDataFileID {
		return db.dataFile, nil
	}

	df, ok := db.readFiles[keysFileID]

	if !ok {
		// dontCreateIfMissing: the id came from the keydir, so the file exists.
		// Creating an empty one would turn a missing file into a silent wrong
		// answer rather than an error.
		reopenedDF, reopenErr := OpenDataFile(db.directory, keysFileID, dontCreateIfMissing)
		if reopenErr != nil {
			return nil, reopenErr
		}

		// kept open, so later reads of the same file cost one syscall, not three
		db.readFiles[keysFileID] = reopenedDF
		df = reopenedDF
	}

	return df, nil

}

// Close flushes the data file whatever the policy says, then closes it. No
// poisoned check: a database that cannot be closed is one whose descriptor leaks.
func (db *DB) Close() error {
	syncErr := db.sync()

	// closed even when the flush failed, or the descriptor leaks on exactly the
	// path where something has already gone wrong
	closeErr := db.dataFile.Close()

	// every sealed handle too, and all of them even if one fails, so a single
	// bad handle cannot strand the rest
	for id, df := range db.readFiles {
		err := df.Close()
		if err != nil && closeErr == nil {
			closeErr = err
		}

		delete(db.readFiles, id)
	}

	// the sync error wins: it means data may be lost, where a close error
	// usually means only that the handle was already gone
	if syncErr != nil {
		return syncErr
	}

	return closeErr
}

// Delete removes key by appending a tombstone, since an append-only file cannot
// erase. The keydir drop hides it now; the tombstone stops recovery reviving it.
func (db *DB) Delete(key []byte) error {
	// a delete is a write, so it is refused for the same reason a Put is
	if db.poisoned != nil {
		return db.poisoned
	}

	tombstone := &Record{
		Timestamp: db.nextTimestamp(),
		// bit 0 set — this is what marks the deletion, not the empty value,
		// since Put(key, []byte{}) writes an empty value too
		Flags: flagTombstone,
		Key:   key,
		// nothing to store: len(nil) is 0, so this record is header + key only
		Value: nil,
	}

	rotateErr := db.rotateIfFull(RecordSize(len(key), 0))

	if rotateErr != nil {
		return rotateErr
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

// rotateIfFull seals the active file when the next record of size bytes would
// take it past the threshold. Checked before the append, so the limit is
// respected rather than overshot.
func (db *DB) rotateIfFull(size int) error {

	currDataFile := db.dataFile

	// an empty file takes the record whatever its size: one larger than the
	// threshold has nowhere else to go, and rotating first would only leave an
	// empty file behind
	if currDataFile.offset == 0 || currDataFile.offset+int64(size) <= db.maxFileSize {
		return nil
	}

	return db.rotate()

}

// rotate seals the active file and starts the next one. The sealed file stays
// open for reading, since keydir entries still point into it.
func (db *DB) rotate() error {

	currDataFile := db.dataFile

	currID := currDataFile.id

	// flushed while db.dataFile still points at the file being sealed — one
	// that will never be written again is a durability point
	err := db.sync()
	if err != nil {
		return err
	}

	// from the shared allocator, not currID + 1: merge reserves ids too
	newDataFile, err := OpenDataFile(db.directory, db.nextFileID(), createIfMissing)

	if err != nil {
		return err
	}

	// both together, and only once the successor exists: a failed open leaves
	// the active file exactly where it was
	db.readFiles[currID] = currDataFile
	db.dataFile = newDataFile
	return nil

}
