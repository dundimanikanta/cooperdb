package cooperdb

// Entry locates the newest record for one key: Offset is where that record
// starts, and Size covers the whole record, header included.
type Entry struct {
	FileID    uint32
	Offset    int64
	Size      int
	Timestamp int64
}

// KeyDir maps every live key to the location of its newest record; the map is
// unexported so that every access has to go through the methods below.
type KeyDir struct {
	entries map[string]Entry
}

// NewKeyDir returns an empty keydir with its map ready to use.
func NewKeyDir() *KeyDir {
	keyd := &KeyDir{entries: make(map[string]Entry)}
	return keyd
}

// Get returns the entry stored for key, and whether there was one.
func (k *KeyDir) Get(key []byte) (Entry, bool) {
	e, ok := k.entries[string(key)]
	return e, ok
}

// Put records where key's newest record lives, replacing any earlier entry.
func (k *KeyDir) Put(key []byte, e Entry) {
	k.entries[string(key)] = e
}

// Delete drops key from the keydir; deleting an absent key is a no-op.
func (k *KeyDir) Delete(key []byte) {
	delete(k.entries, string(key))
}

// Len reports how many live keys the keydir is holding.
func (k *KeyDir) Len() int {
	return len(k.entries)
}
