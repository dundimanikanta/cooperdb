package cooperdb

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
