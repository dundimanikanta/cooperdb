// Package loadgen is the shared half of cooperdb's load tests: the workload,
// latency histograms, merging, and reporting. Each program under cmd/ supplies
// only its own request loop.
package loadgen

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dundimanikanta/cooperdb"
)

type Config struct {
	Name        string
	Dir         string
	Keep        bool
	Clients     int
	Keys        int
	ValueSize   int
	Mix         string
	Zipf        bool
	Warmup      time.Duration
	Duration    time.Duration
	Window      time.Duration
	MergeEvery  time.Duration
	Sync        string
	SyncEvery   int
	MaxFileSize int64
	ResultsDir  string
	Seed        int64

	// requests per second across all clients; 0 means as fast as possible
	Rate float64

	readPct, putPct, deletePct int
}

func DefaultConfig(name string) *Config {
	return &Config{
		Name:        name,
		Clients:     16,
		Keys:        100_000,
		ValueSize:   100,
		Mix:         "80/15/5",
		Warmup:      time.Minute,
		Duration:    10 * time.Minute,
		Window:      10 * time.Second,
		MergeEvery:  30 * time.Second,
		Sync:        "never",
		SyncEvery:   100,
		MaxFileSize: 64 << 20,
		ResultsDir:  "bench/results",
		Seed:        1,
	}
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Dir, "dir", c.Dir, "database directory (default: a new temporary directory)")
	fs.BoolVar(&c.Keep, "keep", c.Keep, "keep the database directory afterwards")
	fs.IntVar(&c.Clients, "clients", c.Clients, "concurrent client goroutines")
	fs.IntVar(&c.Keys, "keys", c.Keys, "size of the keyspace, all loaded before the run")
	fs.IntVar(&c.ValueSize, "value", c.ValueSize, "value size in bytes")
	fs.StringVar(&c.Mix, "mix", c.Mix, "read/put/delete percentages")
	fs.BoolVar(&c.Zipf, "zipf", c.Zipf, "pick keys from a Zipf distribution instead of uniformly")
	fs.DurationVar(&c.Warmup, "warmup", c.Warmup, "load before measuring starts, reported but not counted")
	fs.DurationVar(&c.Duration, "duration", c.Duration, "measured run length")
	fs.DurationVar(&c.Window, "window", c.Window, "reporting interval")
	fs.DurationVar(&c.MergeEvery, "merge-every", c.MergeEvery, "time between merges")
	fs.StringVar(&c.Sync, "sync", c.Sync, "never, always, or every (with -sync-every)")
	fs.IntVar(&c.SyncEvery, "sync-every", c.SyncEvery, "writes per flush when -sync=every")
	fs.Int64Var(&c.MaxFileSize, "max-file-size", c.MaxFileSize, "bytes before the active file is sealed")
	fs.StringVar(&c.ResultsDir, "results", c.ResultsDir, "directory for the CSV and the text report")
	fs.Int64Var(&c.Seed, "seed", c.Seed, "random seed for key and operation choice")
}

func (c *Config) validate() error {
	parts := strings.Split(c.Mix, "/")
	if len(parts) != 3 {
		return fmt.Errorf("-mix %q: want three percentages, like 80/15/5", c.Mix)
	}

	pcts := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return fmt.Errorf("-mix %q: %q is not a percentage", c.Mix, p)
		}
		pcts[i] = n
	}

	if pcts[0]+pcts[1]+pcts[2] != 100 {
		return fmt.Errorf("-mix %q: percentages add up to %d, not 100", c.Mix, pcts[0]+pcts[1]+pcts[2])
	}

	c.readPct, c.putPct, c.deletePct = pcts[0], pcts[1], pcts[2]

	// a value starts with its own key, so it has to be longer than one
	if c.ValueSize < len(keyFor(0))+1 {
		return fmt.Errorf("-value %d: must be at least %d bytes", c.ValueSize, len(keyFor(0))+1)
	}

	if c.Clients < 1 || c.Keys < 1 || c.Window <= 0 || c.Duration <= 0 || c.Warmup < 0 {
		return fmt.Errorf("-clients, -keys, -window and -duration must be positive")
	}

	// keys are a fixed 10 bytes, which is what makes the value-prefix check exact
	if c.Keys > 10_000_000 {
		return fmt.Errorf("-keys %d: at most 10,000,000", c.Keys)
	}

	if _, err := c.syncOption(); err != nil {
		return err
	}

	return nil
}

func (c *Config) syncOption() (cooperdb.Option, error) {
	switch c.Sync {
	case "never":
		return cooperdb.WithSyncNever(), nil
	case "always":
		return cooperdb.WithSyncAlways(), nil
	case "every":
		return cooperdb.WithSyncEveryN(c.SyncEvery), nil
	}
	return nil, fmt.Errorf("-sync %q: want never, always or every", c.Sync)
}

func (c *Config) syncLabel() string {
	if c.Sync == "every" {
		return fmt.Sprintf("every%d", c.SyncEvery)
	}
	return c.Sync
}

func keyFor(i int) []byte {
	return []byte(fmt.Sprintf("key%07d", i))
}
