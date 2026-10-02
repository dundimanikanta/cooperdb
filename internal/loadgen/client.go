package loadgen

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/dundimanikanta/cooperdb"
)

type Op int

const (
	OpGet Op = iota
	OpPut
	OpDelete
)

// Client is one goroutine's share of the load. Its histograms are its own, so
// recording a latency never contends with another client.
type Client struct {
	ID int

	db    *cooperdb.DB
	cfg   *Config
	keys  [][]byte
	rng   *rand.Rand
	zipf  *rand.Zipf
	value []byte
	stop  *atomic.Bool

	getHit, getMiss, put, del Histogram
}

func newClient(id int, db *cooperdb.DB, cfg *Config, keys [][]byte, stop *atomic.Bool) *Client {
	rng := rand.New(rand.NewSource(cfg.Seed + int64(id)))

	c := &Client{
		ID:    id,
		db:    db,
		cfg:   cfg,
		keys:  keys,
		rng:   rng,
		value: valueBuffer(cfg.ValueSize),
		stop:  stop,
	}

	if cfg.Zipf {
		c.zipf = rand.NewZipf(rng, 1.1, 1, uint64(len(keys)-1))
	}

	return c
}

// Stopped reports whether the run is over. An atomic flag rather than a context:
// ctx.Err takes a mutex, which 16 clients would contend on every request.
func (c *Client) Stopped() bool {
	return c.stop.Load()
}

// Next picks the next key and operation according to the mix.
func (c *Client) Next() (Op, []byte) {
	var k int
	if c.zipf != nil {
		k = int(c.zipf.Uint64())
	} else {
		k = c.rng.Intn(len(c.keys))
	}

	key := c.keys[k]

	switch r := c.rng.Intn(100); {
	case r < c.cfg.readPct:
		return OpGet, key
	case r < c.cfg.readPct+c.cfg.putPct:
		return OpPut, key
	default:
		return OpDelete, key
	}
}

// Do performs op on key and records its latency measured from start, which the
// caller chooses: when the request was sent, or when it was scheduled to be.
func (c *Client) Do(op Op, key []byte, start time.Time) error {
	switch op {
	case OpGet:
		v, err := c.db.Get(key)
		elapsed := time.Since(start)

		if errors.Is(err, cooperdb.ErrKeyNotFound) {
			c.getMiss.Record(elapsed)
			return nil
		}
		if err != nil {
			return fmt.Errorf("get %s: %w", key, err)
		}

		// every value starts with its own key, so a read served another key's value is caught here
		if len(v) != c.cfg.ValueSize || !bytes.HasPrefix(v, key) {
			return fmt.Errorf("get %s returned a value that is not its own: %.24q", key, v)
		}

		c.getHit.Record(elapsed)

	case OpPut:
		copy(c.value, key)
		err := c.db.Put(key, c.value)
		elapsed := time.Since(start)

		if err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}
		c.put.Record(elapsed)

	case OpDelete:
		err := c.db.Delete(key)
		elapsed := time.Since(start)

		if err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
		c.del.Record(elapsed)
	}

	return nil
}

// valueBuffer is a value with room for a key at the front; the rest is filler.
func valueBuffer(size int) []byte {
	return bytes.Repeat([]byte{'.'}, size)
}

func valueFor(key []byte, size int) []byte {
	v := valueBuffer(size)
	copy(v, key)
	return v
}
