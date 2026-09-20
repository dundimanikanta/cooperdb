package cooperdb

import (
	"path/filepath"
	"strconv"
	"strings"
)

// Recovery rebuilds the keydir from the data files on disk, so Put and Delete
// survive a restart. Its correctness rests entirely on ordering: files in
// creation order, records in offset order, or an older value wins.

// dataFileIDs returns the ids of every data file in dir, in creation order.
// The zero-padding OpenDataFile applies is what lets a text sort give that
// order — "000010" sorts after "000009", where "10" would sort before "9".
func dataFileIDs(dir string) ([]uint32, error) {
	// Glob returns its matches already sorted, and the zero-padding makes that
	// lexical order the same as creation order
	pattern := filepath.Join(dir, "*.data")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	ids := make([]uint32, 0, len(matches))

	for _, match := range matches {
		// "/tmp/db/000003.data" becomes "000003"
		name := strings.TrimSuffix(filepath.Base(match), ".data")

		// a name that is not an id was not written by us; skip it rather than
		// fail, so one stray file cannot stop the database opening at all
		id, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue
		}

		ids = append(ids, uint32(id))
	}

	return ids, nil
}

// loadKeyDir replays every data file in dir and returns the keydir it
// reconstructs, the highest file id seen so the caller knows which file to
// reopen as active, and that file's valid-data length — which is NOT its size
// when a crash left a partial record at the tail.
func loadKeyDir(dir string) (*KeyDir, uint32, int64, int64, error) {
	// an empty directory is a new database, not a failure, so these are what a
	// caller gets when the loop below never runs
	kd := NewKeyDir()
	highestID := uint32(0)
	activeValidLen := int64(0)
	maxTimestamp := int64(0)

	ids, err := dataFileIDs(dir)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	// creation order, because a later file's records must land last
	for _, id := range ids {
		// replay only reads, and dataFileIDs just saw these files, so a missing
		// one is a real problem rather than something to create and scan empty
		df, err := OpenDataFile(dir, id, dontCreateIfMissing)
		if err != nil {
			return nil, 0, 0, 0, err
		}

		// id is captured from this iteration, so every record gets the file it
		// was actually read from
		validLen, err := df.Scan(func(offset int64, r *Record) error {
			applyRecord(kd, id, offset, r)
			if r.Timestamp > maxTimestamp {
				maxTimestamp = r.Timestamp
			}
			return nil
		})

		// closed either way: a scan failure must not leak the descriptor, and
		// holding every file open would not scale past a handful of them
		df.Close()

		if err != nil {
			return nil, 0, 0, 0, err
		}

		// ids arrive sorted, so the last one seen is the highest — and the last
		// iteration's valid length belongs to the file that becomes active
		highestID = id
		activeValidLen = validLen
	}

	// the tombstone entries have done their work; what is handed back holds live
	// keys only, which is what Get, Len and merge all assume
	kd.dropTombstones()

	return kd, highestID, activeValidLen, maxTimestamp, nil
}

// applyRecord folds one replayed record into the keydir.
//
// Records arrive oldest first, so the last one to touch a key wins by virtue of
// arriving last. The timestamp comparison exists for when that stops being true:
// merge in week 3 rewrites old records into newly created files, so file order
// no longer implies record age.
func applyRecord(kd *KeyDir, fileID uint32, offset int64, r *Record) {
	// strictly newer, so equal timestamps fall through and the record arriving
	// later still wins — two writes can land in the same nanosecond, and
	// dropping the second would be a lost write
	existing, ok := kd.Get(r.Key)
	if ok && existing.Timestamp > r.Timestamp {
		return
	}

	// by the flag, never by an empty value: Put(key, nil) stores an empty value
	// and must not be mistaken for a delete
	if r.IsTombstone() {
		// stored rather than dropped, so the deletion's timestamp survives to be
		// compared against. Removing the entry would leave nothing for the guard
		// above to fire on, and merge rewrites old records into newly created
		// files — so a put older than this tombstone can still arrive after it.
		kd.Put(r.Key, Entry{Timestamp: r.Timestamp, Tombstone: true})
		return
	}

	// where this record lives, not what it holds — the value stays on disk
	kd.Put(r.Key, Entry{
		FileID:    fileID,
		Offset:    offset,
		Size:      RecordSize(len(r.Key), len(r.Value)),
		Timestamp: r.Timestamp,
	})
}
