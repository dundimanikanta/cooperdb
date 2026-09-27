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

	db.mu.RLock()
	activeFileID := db.dataFile.id
	db.mu.RUnlock()

	// filter out the active file id from the list of mergeable files
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

	db.mu.RLock()
	defer db.mu.RUnlock()

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

	db.mu.Lock()
	id := db.nextFileID()
	db.mu.Unlock()

	output, err := OpenMergeFile(db.directory, id, createIfMissing)

	if err != nil {
		return nil, nil, err
	}

	for _, id := range inputs {

		// aborts rather than skips: the caller deletes these files
		input, err := OpenDataFile(db.directory, id, dontCreateIfMissing)
		if err != nil {
			output.Close()
			os.Remove(filepath.Join(db.directory, fmt.Sprintf("%06d.merge.tmp", output.id)))
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
			os.Remove(filepath.Join(db.directory, fmt.Sprintf("%06d.merge.tmp", output.id)))
			return nil, nil, err
		}
	}

	return output, relocations, nil
}

func OpenMergeFile(dir string, id uint32, create bool) (*DataFile, error) {
	// zero-padded so the filenames sort lexically in creation order
	path := filepath.Join(dir, fmt.Sprintf("%06d.merge.tmp", id))

	// O_APPEND sends every write to EOF atomically; O_RDWR because reads come
	// back through this same handle
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND

	// read-only and no O_CREATE: a missing file errors instead of being made
	// empty, and the kernel refuses a write rather than trusting nobody tries
	if !create {
		flags = os.O_RDONLY
	}

	f, err := os.OpenFile(path, flags, dataFilePerm)
	if err != nil {
		return nil, err
	}

	// metadata snapshot of the file behind this descriptor, not of the path
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	// starting at the end of what is already there, never at 0
	offset := fi.Size()

	return &DataFile{
		file:   f,
		id:     id,
		offset: offset,
	}, nil
}

// applyRelocations points each copied key at its new home, skipping any the keydir
// no longer names, and reports how many were applied.
func (db *DB) applyRelocations(relocations map[string]Entry) int {

	// how many were actually repointed
	applied := 0

	db.mu.Lock()
	defer db.mu.Unlock()

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

	db.readFilesMu.Lock()
	defer db.readFilesMu.Unlock()

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

// Merge reclaims the space held by superseded records and tombstones: every live
// record in the sealed files is copied into one new file, and the old ones deleted.
func (db *DB) Merge() error {

	if !db.mergeMu.TryLock() {
		return ErrMergeInProgress
	}
	defer db.mergeMu.Unlock()

	// a failed sync means what is on disk is unknown, so nothing can be trusted
	db.mu.RLock()
	poisoned := db.poisoned
	db.mu.RUnlock()

	if poisoned != nil {
		return poisoned
	}

	mergeableFiles, err := db.mergeableFiles()

	if err != nil {
		return err
	}

	if len(mergeableFiles) == 0 {
		return nil
	}

	mergeFile, relocations, err := db.copyLiveRecords(mergeableFiles)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(db.directory, fmt.Sprintf("%06d.merge.tmp", mergeFile.id))
	finalPath := filepath.Join(db.directory, fmt.Sprintf("%06d.data", mergeFile.id))

	if err := mergeFile.Sync(); err != nil {
		mergeFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		mergeFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := syncDir(db.directory); err != nil {
		return err
	}

	db.applyRelocations(relocations)

	mergeFile.Close()

	return db.deleteAlreadyMergedFiles(mergeableFiles)
}

// syncDir flushes the directory itself, which is what makes a file's *name*
// durable. Syncing a file's contents says nothing about whether the directory
// entry naming it survived a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = d.Sync()
	d.Close()

	return err
}
