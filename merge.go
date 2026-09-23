package cooperdb

import (
	"fmt"
	"os"
	"path/filepath"
)

// mergeableFiles returns the ids of every data file except the active one,
// which is excluded because it is still being appended to.
func (db *DB) mergeableFiles() ([]uint32, error) {

	ids, err := dataFileIDs(db.directory)
	if err != nil {
		return nil, err
	}

	// filter out the active file id from the list of mergeable files
	activeFileID := db.dataFile.id

	for i, id := range ids {
		if id == activeFileID {
			ids = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	return ids, nil
}

// isLive reports whether the record at fileID/offset is the version the keydir
// currently serves for its key. Superseded and deleted records answer false.
func (db *DB) isLive(fileID uint32, offset int64, r *Record) bool {
	e, ok := db.keyDirectory.Get(r.Key)
	if !ok {
		return false
	}

	if e.Tombstone {
		return false
	}

	if e.FileID != fileID || e.Offset != offset {
		return false
	}
	return true

}

// copyLiveRecords walks every input file in order and appends the records worth
// keeping to one new output file, reporting where each of them landed.
func (db *DB) copyLiveRecords(inputs []uint32) (*DataFile, map[string]Entry, error) {

	if len(inputs) == 0 {
		return nil, nil, nil
	}

	relocations := make(map[string]Entry)

	output, err := OpenDataFile(db.directory, db.nextFileID(), createIfMissing)
	if err != nil {
		return nil, nil, err
	}

	for _, id := range inputs {

		// aborts rather than skips: the caller deletes these files
		input, err := OpenDataFile(db.directory, id, dontCreateIfMissing)
		if err != nil {
			output.Close()
			os.Remove(filepath.Join(db.directory, fmt.Sprintf("%06d.data", output.id)))
			return nil, nil, err
		}

		_, err = input.Scan(func(offset int64, r *Record) error {

			// copied whatever the keydir says, but never a relocation: a deleted
			// key has no entry to repoint
			if r.IsTombstone() {
				_, appendErr := output.Append(r)
				return appendErr
			}

			if !db.isLive(id, offset, r) {
				return nil
			}

			// unchanged: a fresh timestamp would beat the live record on replay
			newOffset, appendErr := output.Append(r)
			if appendErr != nil {
				return appendErr
			}

			relocations[string(r.Key)] = Entry{
				FileID:    output.id,
				Offset:    newOffset,
				Size:      RecordSize(len(r.Key), len(r.Value)),
				Timestamp: r.Timestamp,
			}

			return nil
		})

		input.Close()

		if err != nil {
			output.Close()
			os.Remove(filepath.Join(db.directory, fmt.Sprintf("%06d.data", output.id)))
			return nil, nil, err
		}
	}

	return output, relocations, nil
}

// applyRelocations points each copied key at its new home, skipping any the keydir
// no longer names, and reports how many were applied.
func (db *DB) applyRelocations(relocations map[string]Entry) int {

	// how many were actually repointed
	applied := 0

	// iterating over the the relocations map
	for key, moved := range relocations {

		current, ok := db.keyDirectory.Get([]byte(key))

		// checking if anything has been chnaged in the active keydir during the merge
		// either deleted or updated we skip the copying to active keydir
		if !ok || current.Timestamp != moved.Timestamp {
			continue
		}

		db.keyDirectory.Put([]byte(key), moved)
		applied++
	}

	return applied
}

// deleteAlreadyMergedFiles deltes the files that have been merged 

func (db *DB) deleteAlreadyMergedFiles(inputs []uint32) error {

	// the first failure, held while the rest are still attempted
	var firstErr error

	for _, id := range inputs {

		// only files a read has actually opened are in the cache
		df, ok := db.readFiles[id]

		if ok {
			err := df.Close()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			delete(db.readFiles, id)
		}

		err := os.Remove(filepath.Join(db.directory, fmt.Sprintf("%06d.data", id)))
		if err != nil && firstErr == nil {
			firstErr = err
		}

	}

	return firstErr
}
