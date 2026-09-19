package cooperdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// dataFilePerm is the permission mode requested when a data file is created.
// Owner read/write, everyone else read-only. Filtered through the process
// umask, and only consulted when the file does not already exist.
const dataFilePerm os.FileMode = 0644

// Arguments for OpenDataFile's create parameter, so a call site says which case
// it means rather than a bare true or false.
const (
	createIfMissing     = true
	dontCreateIfMissing = false
)

// DataFile is one append-only log file. Records are only ever added at the
// end; nothing already written is modified in place.
//
// The write offset is tracked here rather than read from the file handle
// because O_APPEND makes the kernel jump to end-of-file on every write, so the
// handle's own position says nothing useful about where the next record lands.
// ErrDataFileDamaged is returned once a failed write leaves bytes behind that
// could not be removed. The file's true length is then unknown, so any offset
// this DataFile reported afterwards would be wrong.
var ErrDataFileDamaged = errors.New("cooperdb: data file length unknown after a failed write")

type DataFile struct {
	file    *os.File
	id      uint32
	offset  int64
	damaged bool // a failed write could not be cleaned up; offset is unreliable
}

// OpenDataFile opens the data file with the given id inside dir, positioning the
// write offset at the end of what is already there. createIfMissing opens it for
// appending; dontCreateIfMissing opens it read-only and errors when it is absent.
func OpenDataFile(dir string, id uint32, create bool) (*DataFile, error) {
	// zero-padded so the filenames sort lexically in creation order
	path := filepath.Join(dir, fmt.Sprintf("%06d.data", id))

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

// Append encodes r, writes it to the end of the file, and returns the offset
// the record starts at. That offset is what the keydir stores.
func (d *DataFile) Append(r *Record) (int64, error) {
	// once the length is unknown, every offset returned from here is a guess
	if d.damaged {
		return 0, ErrDataFileDamaged
	}

	// lay the record out as header | key | value
	encoded := r.Encode()

	// the reader's coordinate, unrecoverable once the write moves d.offset past it
	currentOffset := d.offset

	// Write reports a non-nil error whenever n < len(encoded), so err covers it

	file := d.file
	n, err := file.Write(encoded)

	if err != nil {
		// bytes can land even on a failed write, and O_APPEND sends the *next*
		// write to the physical end of file — so leaving them would make
		// d.offset lie about where every later record goes. Cutting back to
		// where this record began restores the state before the write.
		//
		// Safe to discard: Append is returning an error, so no caller was ever
		// told this record exists.
		if truncErr := file.Truncate(currentOffset); truncErr != nil {
			d.damaged = true
			return 0, truncErr
		}

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

// ScanRecordAt reads the record at offset and reports the bytes it occupied,
// working its size out from the header instead of being told it. io.EOF ends a
// scan cleanly.
func (d *DataFile) ScanRecordAt(offset int64) (*Record, int, error) {
	// raw bytes, not d.ReadAt: that one decodes, and a header alone never
	// satisfies Decode, which needs the key and value too
	header := make([]byte, headerSize)

	// io.EOF covers both "nothing left" and "fewer than a header remains",
	// since os.File.ReadAt reports EOF for a partial fill as well
	_, err := d.file.ReadAt(header, offset)
	if errors.Is(err, io.EOF) {
		return nil, 0, io.EOF
	}

	if err != nil {
		return nil, 0, err
	}

	// the two size fields sit where Decode reads them from
	keySize := binary.LittleEndian.Uint32(header[12:16])
	valueSize := binary.LittleEndian.Uint32(header[16:20])

	// now the size is known, this read goes through the decoding path so the
	// crc is verified
	recordSize := RecordSize(int(keySize), int(valueSize))

	record, err := d.ReadAt(offset, recordSize)
	if err != nil {
		return nil, 0, err
	}

	return record, recordSize, nil
}

// Scan walks every record in the file from the start, calling fn with each one
// and the offset it was found at. It stops cleanly at the end of the file, and
// also at the first record that is incomplete or fails its checksum — after a
// crash the tail of a file is expected to be damaged, and everything before it
// is still good.
//
// fn returning an error stops the scan and passes that error back.
func (d *DataFile) Scan(fn func(offset int64, r *Record) error) error {
	// records sit end to end from the start of the file, so the walk begins at 0
	offset := int64(0)

	for {
		record, size, err := d.ScanRecordAt(offset)

		// a damaged or half-written tail is what a crash leaves behind, so it
		// ends the scan rather than failing it — everything before it is good
		if errors.Is(err, io.EOF) ||
			errors.Is(err, io.ErrUnexpectedEOF) ||
			errors.Is(err, ErrShortRecord) ||
			errors.Is(err, ErrCorruptRecord) {
			return nil
		}

		// anything else is a real I/O failure, and recovery should not pretend
		// it read a whole file when it did not
		if err != nil {
			return err
		}

		// the caller decides what the record means; its error stops the walk
		err = fn(offset, record)
		if err != nil {
			return err
		}

		// the record just read ends exactly where the next one begins
		offset += int64(size)
	}
}
