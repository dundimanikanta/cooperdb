package loadgen

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dundimanikanta/cooperdb"
)

// Loop is one client's request loop. It returns when c.Stopped() is true, or
// with the first error, which ends the whole run.
type Loop func(c *Client) error

var errInterrupted = errors.New("interrupted")

type mergeEvent struct {
	start, end time.Time
	done       bool
}

type run struct {
	cfg *Config
	db  *cooperdb.DB
	dir string
	out io.Writer

	stop     atomic.Bool
	failOnce sync.Once
	failed   chan struct{}
	failErr  error

	mergeMu sync.Mutex
	merges  []mergeEvent
}

// Run loads the database, drives it with one goroutine per client running loop,
// merges on a schedule, and reports every window until the run ends or fails.
func Run(cfg *Config, loop Loop) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	if err := os.MkdirAll(cfg.ResultsDir, 0o755); err != nil {
		return err
	}

	base := filepath.Join(cfg.ResultsDir,
		fmt.Sprintf("%s-sync-%s-%s", cfg.Name, cfg.syncLabel(), time.Now().Format("20060102-150405")))

	txt, err := os.Create(base + ".txt")
	if err != nil {
		return err
	}
	defer txt.Close()

	out := io.MultiWriter(os.Stdout, txt)

	dir := cfg.Dir
	if dir == "" {
		dir, err = os.MkdirTemp("", "cooperdb-load-")
		if err != nil {
			return err
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	if !cfg.Keep {
		defer os.RemoveAll(dir)
	}

	if free, ok := freeBytes(dir); ok && free < 5<<30 {
		return fmt.Errorf("only %.1f GB free under %s; a run can need about 5 GB", float64(free)/1e9, dir)
	}

	syncOpt, _ := cfg.syncOption()

	db, err := cooperdb.Open(dir, syncOpt, cooperdb.WithMaxFileSize(cfg.MaxFileSize))
	if err != nil {
		return err
	}
	defer db.Close()

	r := &run{cfg: cfg, db: db, dir: dir, out: out, failed: make(chan struct{})}
	r.printConfig(base)

	// Ctrl-C takes the same path as a failure, so the clients stop, any merge
	// finishes, and the database folder is still deleted
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)

	runDone := make(chan struct{})
	defer close(runDone)

	go func() {
		select {
		case <-sigs:
			// a second Ctrl-C now kills the program outright
			signal.Stop(sigs)
			fmt.Fprintln(out, "\ninterrupted: stopping the clients and waiting for any merge in progress, then deleting the database (Ctrl-C again to quit at once, leaving it behind)")
			r.fail(errInterrupted)
		case <-runDone:
		}
	}()

	keys := make([][]byte, cfg.Keys)
	for i := range keys {
		keys[i] = keyFor(i)
	}

	preloadStart := time.Now()
	for _, k := range keys {
		if err := db.Put(k, valueFor(k, cfg.ValueSize)); err != nil {
			return fmt.Errorf("preload: %w", err)
		}
	}
	fmt.Fprintf(out, "preloaded %s keys in %v\n\n", commas(int64(len(keys))), time.Since(preloadStart).Round(time.Millisecond))

	clients := make([]*Client, cfg.Clients)
	for i := range clients {
		clients[i] = newClient(i, db, cfg, keys, &r.stop)
	}

	gc := newGCStats()

	var wg sync.WaitGroup
	start := time.Now()

	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			if err := loop(c); err != nil {
				r.fail(err)
			}
		}(c)
	}

	stopMerger := make(chan struct{})
	mergerDone := make(chan struct{})
	go r.mergeLoop(stopMerger, mergerDone)

	rows := r.report(clients, start, gc)

	r.stop.Store(true)
	wg.Wait()
	close(stopMerger)
	<-mergerDone

	if r.failErr != nil {
		// an interrupted run is abandoned on purpose, so its partial report is
		// removed rather than left among the results that get committed
		if errors.Is(r.failErr, errInterrupted) {
			db.Close()
			txt.Close()
			os.Remove(base + ".txt")

			if !cfg.Keep {
				os.RemoveAll(dir)
				fmt.Printf("deleted %s and the partial report\n", dir)
			}
		}
		return r.failErr
	}

	live, err := liveBytes(db, keys)
	if err != nil {
		return err
	}

	beforeFinal := diskBytes(dir)
	finalStart := time.Now()

	if err := db.Merge(); err != nil {
		return fmt.Errorf("final merge: %w", err)
	}

	final := finalMerge{before: beforeFinal, after: diskBytes(dir), took: time.Since(finalStart)}

	r.summary(rows, live, final, base)

	if err := writeCSV(base+".csv", rows, live); err != nil {
		return err
	}

	return db.Close()
}

func (r *run) fail(err error) {
	r.failOnce.Do(func() {
		r.failErr = err
		r.stop.Store(true)
		close(r.failed)
	})
}

// mergeLoop merges every MergeEvery. A merge that outlasts the interval just
// means the next one starts straight after it, since a ticker drops missed ticks.
func (r *run) mergeLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	t := time.NewTicker(r.cfg.MergeEvery)
	defer t.Stop()

	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		if r.stop.Load() {
			return
		}

		r.mergeMu.Lock()
		r.merges = append(r.merges, mergeEvent{start: time.Now()})
		i := len(r.merges) - 1
		r.mergeMu.Unlock()

		err := r.db.Merge()

		r.mergeMu.Lock()
		r.merges[i].end, r.merges[i].done = time.Now(), true
		r.mergeMu.Unlock()

		if err != nil {
			r.fail(fmt.Errorf("merge: %w", err))
			return
		}
	}
}

// mergeBusy is how much of [from, to) had a merge running.
func (r *run) mergeBusy(from, to time.Time) time.Duration {
	r.mergeMu.Lock()
	defer r.mergeMu.Unlock()

	var busy time.Duration
	for _, m := range r.merges {
		end := m.end
		if !m.done {
			end = to
		}

		lo, hi := m.start, end
		if from.After(lo) {
			lo = from
		}
		if to.Before(hi) {
			hi = to
		}

		if hi.After(lo) {
			busy += hi.Sub(lo)
		}
	}
	return busy
}

// mergeDurations lists every finished merge's length.
func (r *run) mergeDurations() []time.Duration {
	r.mergeMu.Lock()
	defer r.mergeMu.Unlock()

	var ds []time.Duration
	for _, m := range r.merges {
		if m.done {
			ds = append(ds, m.end.Sub(m.start))
		}
	}
	return ds
}

// liveBytes is the size the live data would take as records: every key still
// present, read back once after the load stops.
func liveBytes(db *cooperdb.DB, keys [][]byte) (int64, error) {
	var total int64
	for _, k := range keys {
		v, err := db.Get(k)
		if errors.Is(err, cooperdb.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("measuring live data: %w", err)
		}
		total += int64(cooperdb.RecordSize(len(k), len(v)))
	}
	return total, nil
}

func diskBytes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}

	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil && info.Mode().IsRegular() {
			total += info.Size()
		}
	}
	return total
}
