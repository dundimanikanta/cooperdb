package cooperdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// dataFilePerm is the permission mode requested when a data file is created.
// Owner read/write, everyone else read-only. Filtered through the process
// umask, and only consulted when the file does not already exist.
const dataFilePerm os.FileMode = 0644

// DataFile is one append-only log file. Records are only ever added at the
// end; nothing already written is modified in place.
//
// The write offset is tracked here rather than read from the file handle
// because O_APPEND makes the kernel jump to end-of-file on every write, so the
// handle's own position says nothing useful about where the next record lands.
type DataFile struct {
	file   *os.File
	id     uint32
	offset int64
}

// OpenDataFile opens the data file with the given id inside dir, creating it
// if it does not exist, and positions the write offset at the end of whatever
// is already there.
func OpenDataFile(dir string, id uint32) (*DataFile, error) {
	// zero-padded so the filenames sort lexically in creation order
	path := filepath.Join(dir, fmt.Sprintf("%06d.data", id))

	// O_APPEND sends every write to EOF atomically; O_RDWR because 1.6 reads back
	// through this same handle
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, dataFilePerm)
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

// Append encodes r, writes it to the end of the file, and returns the offset
// the record starts at. That offset is what the keydir stores.
func (d *DataFile) Append(r *Record) (int64, error) {
	// lay the record out as header | key | value
	encoded := r.Encode()

	// the reader's coordinate, unrecoverable once the write moves d.offset past it
	currentOffset := d.offset

	// Write reports a non-nil error whenever n < len(encoded), so err covers it

	file := d.file
	n, err := file.Write(encoded)

	// a failed write must not advance the offset; partial writes are 2.4/2.5 work
	if err != nil {
		return 0, err
	}

	// the writer's bookmark, advanced past what actually landed
	d.offset = currentOffset + int64(n)

	return currentOffset, nil
}

// Sync flushes the file's contents to stable storage.
//
// Placeholder for now: when this gets called, and what that promises after a
// crash, is the durability question in 2.4. Leave the policy alone until then.
func (d *DataFile) Sync() error {
	// fsync: the page cache is not the disk, and only this closes that gap
	return d.file.Sync()
}

// Close closes the underlying file handle.
func (d *DataFile) Close() error {
	// releases the descriptor only; closing does not imply Sync
	return d.file.Close()
}

// ReadAt reads the single record of the given size starting at offset, and
// decodes it. Both values come from the keydir, which recorded them when the
// record was written.
func (d *DataFile) ReadAt(offset int64, size int) (*Record, error) {
	// the length of the buffer is the read request; ReadAt fills exactly len(b)
	b := make([]byte, size)

	n, err := d.file.ReadAt(b, offset)
	if err != nil {
		return nil, err
	}

	// belt-and-braces: *os.File already errors whenever n < len(b), but the
	// io.ReaderAt contract itself is looser
	if n != size {
		return nil, io.ErrUnexpectedEOF
	}

	// Decode verifies the crc, so a corrupt record fails here rather than
	// surfacing as a plausible-looking value
	return Decode(b)
}
