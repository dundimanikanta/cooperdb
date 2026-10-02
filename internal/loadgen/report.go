package loadgen

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"time"
)

// row is one reporting window.
type row struct {
	at     time.Duration // end of the window, since load started
	length time.Duration
	warmup bool

	getHit, getMiss, put, del Snapshot

	diskBytes  int64
	mergeBusy  time.Duration
	gcCycles   uint64
	gcMaxPause time.Duration
}

func (w *row) reads() uint64  { return w.getHit.Count() + w.getMiss.Count() }
func (w *row) writes() uint64 { return w.put.Count() + w.del.Count() }

func (w *row) readLatency() *Snapshot {
	var s Snapshot
	s.Add(&w.getHit)
	s.Add(&w.getMiss)
	return &s
}

func (w *row) writeLatency() *Snapshot {
	var s Snapshot
	s.Add(&w.put)
	s.Add(&w.del)
	return &s
}

func perSecond(n uint64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

func (w *row) hitPct() float64 {
	if w.reads() == 0 {
		return 0
	}
	return 100 * float64(w.getHit.Count()) / float64(w.reads())
}

// report drains every client once per window and prints a line, until the run
// has lasted Warmup+Duration or a client or merge has failed.
func (r *run) report(clients []*Client, start time.Time, gc *gcStats) []*row {
	total := r.cfg.Warmup + r.cfg.Duration
	windows := int(math.Ceil(float64(total) / float64(r.cfg.Window)))

	ticker := time.NewTicker(r.cfg.Window)
	defer ticker.Stop()

	r.printHeader()

	var rows []*row
	prev := start

	for n := 1; n <= windows; n++ {
		select {
		case <-ticker.C:
		case <-r.failed:
			return rows
		}

		now := time.Now()

		w := &row{
			at:     now.Sub(start),
			length: now.Sub(prev),
			warmup: time.Duration(n)*r.cfg.Window <= r.cfg.Warmup,
		}

		for _, c := range clients {
			c.getHit.DrainInto(&w.getHit)
			c.getMiss.DrainInto(&w.getMiss)
			c.put.DrainInto(&w.put)
			c.del.DrainInto(&w.del)
		}

		w.diskBytes = diskBytes(r.dir)
		w.mergeBusy = r.mergeBusy(prev, now)
		w.gcCycles, w.gcMaxPause = gc.sinceLast()

		rows = append(rows, w)
		r.printRow(w)

		prev = now
	}

	return rows
}

func (r *run) printConfig(base string) {
	c := r.cfg

	keys := "uniform keys"
	if c.Zipf {
		keys = "Zipf keys"
	}

	rate := "as fast as possible"
	if c.Rate > 0 {
		rate = fmt.Sprintf("fixed at %s requests/s, latency measured from each request's scheduled start", commas(int64(c.Rate)))
	}

	fmt.Fprintf(r.out, "cooperdb load test: %s\n", c.Name)
	fmt.Fprintf(r.out, "  %d clients · %s keys · %d-byte values · mix %s (read/put/delete) · %s\n",
		c.Clients, commas(int64(c.Keys)), c.ValueSize, c.Mix, keys)
	fmt.Fprintf(r.out, "  sync %s · %d MB files · merge every %v\n", c.syncLabel(), c.MaxFileSize>>20, c.MergeEvery)
	fmt.Fprintf(r.out, "  %v warm-up, then %v measured, reported every %v\n", c.Warmup, c.Duration, c.Window)
	fmt.Fprintf(r.out, "  rate: %s\n", rate)
	fmt.Fprintf(r.out, "  %s %s/%s, %d CPUs · database in %s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), r.dir)
	fmt.Fprintf(r.out, "  results: %s.csv and .txt\n\n", base)
}

func (r *run) printHeader() {
	fmt.Fprintf(r.out, "%6s %-6s | %10s %6s %7s %7s %7s %7s | %9s %7s %7s %7s %7s | %8s %6s | %s\n",
		"time", "phase",
		"reads/s", "hit%", "p50", "p99", "p999", "max",
		"writes/s", "p50", "p99", "p999", "max",
		"disk MB", "merge", "gc")
}

func (r *run) printRow(w *row) {
	phase := "run"
	if w.warmup {
		phase = "warmup"
	}

	rl, wl := w.readLatency(), w.writeLatency()

	merge := "-"
	if w.mergeBusy > 0 {
		merge = fmt.Sprintf("%.1fs", w.mergeBusy.Seconds())
	}

	fmt.Fprintf(r.out, "%5.0fs %-6s | %10s %5.1f%% %7s %7s %7s %7s | %9s %7s %7s %7s %7s | %8.0f %6s | %d, %s\n",
		w.at.Seconds(), phase,
		commas(int64(perSecond(w.reads(), w.length))), w.hitPct(),
		dur(rl.Percentile(50)), dur(rl.Percentile(99)), dur(rl.Percentile(99.9)), dur(rl.Max()),
		commas(int64(perSecond(w.writes(), w.length))),
		dur(wl.Percentile(50)), dur(wl.Percentile(99)), dur(wl.Percentile(99.9)), dur(wl.Max()),
		float64(w.diskBytes)/(1<<20), merge,
		w.gcCycles, dur(w.gcMaxPause))
}

type finalMerge struct {
	before, after int64
	took          time.Duration
}

func (r *run) summary(rows []*row, live int64, final finalMerge, base string) {
	var measured []*row
	for _, w := range rows {
		if !w.warmup {
			measured = append(measured, w)
		}
	}

	if len(measured) == 0 {
		fmt.Fprintln(r.out, "\nno measured windows")
		return
	}

	var t row
	var length, busy time.Duration
	var gcCycles uint64
	var gcWorst time.Duration
	var diskPeak, diskSum int64

	for _, w := range measured {
		t.getHit.Add(&w.getHit)
		t.getMiss.Add(&w.getMiss)
		t.put.Add(&w.put)
		t.del.Add(&w.del)

		length += w.length
		busy += w.mergeBusy
		gcCycles += w.gcCycles
		gcWorst = max(gcWorst, w.gcMaxPause)
		diskPeak = max(diskPeak, w.diskBytes)
		diskSum += w.diskBytes
	}

	rl, wl := t.readLatency(), t.writeLatency()
	o := r.out

	fmt.Fprintf(o, "\n== summary: %v measured ==\n", length.Round(time.Second))
	fmt.Fprintf(o, "operations      %s reads (%.1f%% found), %s writes (%s puts, %s deletes)\n",
		commas(int64(t.reads())), t.hitPct(), commas(int64(t.writes())),
		commas(int64(t.put.Count())), commas(int64(t.del.Count())))
	fmt.Fprintf(o, "throughput      %s reads/s, %s writes/s, %s ops/s in total\n",
		commas(int64(perSecond(t.reads(), length))), commas(int64(perSecond(t.writes(), length))),
		commas(int64(perSecond(t.reads()+t.writes(), length))))
	fmt.Fprintf(o, "read latency    p50 %s · p99 %s · p999 %s · max %s   (found: p99 %s · not found: p99 %s)\n",
		dur(rl.Percentile(50)), dur(rl.Percentile(99)), dur(rl.Percentile(99.9)), dur(rl.Max()),
		dur(t.getHit.Percentile(99)), dur(t.getMiss.Percentile(99)))
	fmt.Fprintf(o, "write latency   p50 %s · p99 %s · p999 %s · max %s   (puts: p99 %s · deletes: p99 %s)\n",
		dur(wl.Percentile(50)), dur(wl.Percentile(99)), dur(wl.Percentile(99.9)), dur(wl.Max()),
		dur(t.put.Percentile(99)), dur(t.del.Percentile(99)))

	ds := r.mergeDurations()
	if len(ds) > 0 {
		slices.Sort(ds)
		fmt.Fprintf(o, "merges          %d in the whole run, each %s to %s (median %s); running %.0f%% of the measured time\n",
			len(ds), dur(ds[0]), dur(ds[len(ds)-1]), dur(ds[len(ds)/2]), 100*busy.Seconds()/length.Seconds())
	} else {
		fmt.Fprintf(o, "merges          none finished\n")
	}

	if live > 0 {
		mean := diskSum / int64(len(measured))
		fmt.Fprintf(o, "disk            live data %.1f MB; on disk peak %.0f MB (%.1fx live), mean %.0f MB (%.1fx)\n",
			mb(live), mb(diskPeak), float64(diskPeak)/float64(live), mb(mean), float64(mean)/float64(live))
		fmt.Fprintf(o, "final merge     %.0f MB -> %.0f MB in %s; still %.1fx live\n",
			mb(final.before), mb(final.after), dur(final.took), float64(final.after)/float64(live))
	}

	fmt.Fprintf(o, "gc              %d cycles, worst pause %s\n", gcCycles, dur(gcWorst))

	// only windows with no merge running, so a slowdown from merging is not taken for heat
	var calm []*row
	for _, w := range measured {
		if w.mergeBusy == 0 {
			calm = append(calm, w)
		}
	}

	if len(calm) >= 2 {
		first, last := calm[0], calm[len(calm)-1]
		firstOps := perSecond(first.reads()+first.writes(), first.length)
		lastOps := perSecond(last.reads()+last.writes(), last.length)
		fmt.Fprintf(o, "heat check      merge-free windows only: %s ops/s at %.0fs, %s ops/s at %.0fs (%+.1f%%)\n",
			commas(int64(firstOps)), first.at.Seconds(), commas(int64(lastOps)), last.at.Seconds(),
			100*(lastOps-firstOps)/firstOps)
	} else {
		fmt.Fprintf(o, "heat check      fewer than two merge-free windows, so heat cannot be told apart from merging\n")
	}
	fmt.Fprintf(o, "results         %s.csv, %s.txt\n", base, base)
}

func writeCSV(path string, rows []*row, live int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Write([]string{
		"elapsed_s", "phase", "reads_per_s", "writes_per_s", "found_pct",
		"read_p50_us", "read_p99_us", "read_p999_us", "read_max_us",
		"write_p50_us", "write_p99_us", "write_p999_us", "write_max_us",
		"get_found_p99_us", "get_missing_p99_us", "put_p99_us", "delete_p99_us",
		"disk_mb", "disk_over_live", "merge_running_s", "gc_cycles", "gc_max_pause_us",
	})

	us := func(d time.Duration) string { return strconv.FormatFloat(float64(d)/1e3, 'f', 1, 64) }
	f1 := func(x float64) string { return strconv.FormatFloat(x, 'f', 1, 64) }

	for _, r := range rows {
		phase := "run"
		if r.warmup {
			phase = "warmup"
		}

		amp := ""
		if live > 0 {
			amp = strconv.FormatFloat(float64(r.diskBytes)/float64(live), 'f', 2, 64)
		}

		rl, wl := r.readLatency(), r.writeLatency()

		w.Write([]string{
			f1(r.at.Seconds()), phase,
			f1(perSecond(r.reads(), r.length)), f1(perSecond(r.writes(), r.length)), f1(r.hitPct()),
			us(rl.Percentile(50)), us(rl.Percentile(99)), us(rl.Percentile(99.9)), us(rl.Max()),
			us(wl.Percentile(50)), us(wl.Percentile(99)), us(wl.Percentile(99.9)), us(wl.Max()),
			us(r.getHit.Percentile(99)), us(r.getMiss.Percentile(99)), us(r.put.Percentile(99)), us(r.del.Percentile(99)),
			f1(mb(r.diskBytes)), amp, f1(r.mergeBusy.Seconds()),
			strconv.FormatUint(r.gcCycles, 10), us(r.gcMaxPause),
		})
	}

	w.Flush()
	return w.Error()
}

// gcStats reads the runtime's own counters, which unlike runtime.ReadMemStats
// do not stop the world to collect.
type gcStats struct {
	samples    []metrics.Sample
	ok         bool
	lastCycles uint64
	lastCounts []uint64
}

func newGCStats() *gcStats {
	g := &gcStats{samples: []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/sched/pauses/total/gc:seconds"},
	}}

	metrics.Read(g.samples)
	g.ok = g.samples[0].Value.Kind() == metrics.KindUint64 &&
		g.samples[1].Value.Kind() == metrics.KindFloat64Histogram

	if g.ok {
		g.sinceLast()
	}
	return g
}

// sinceLast returns the GC cycles since the previous call, and the upper bound
// of the longest stop-the-world pause among them.
func (g *gcStats) sinceLast() (uint64, time.Duration) {
	if !g.ok {
		return 0, 0
	}

	metrics.Read(g.samples)
	cycles := g.samples[0].Value.Uint64()
	h := g.samples[1].Value.Float64Histogram()

	var worst time.Duration
	for i := len(h.Counts) - 1; i >= 0; i-- {
		var prev uint64
		if i < len(g.lastCounts) {
			prev = g.lastCounts[i]
		}

		if h.Counts[i] > prev {
			upper := h.Buckets[i+1]
			if math.IsInf(upper, 1) {
				upper = h.Buckets[i]
			}
			worst = time.Duration(upper * 1e9)
			break
		}
	}

	delta := cycles - g.lastCycles
	g.lastCycles = cycles
	g.lastCounts = append(g.lastCounts[:0], h.Counts...)

	return delta, worst
}

func mb(b int64) float64 {
	return float64(b) / (1 << 20)
}

// dur prints a latency compactly, at a precision that suits its size.
func dur(d time.Duration) string {
	switch {
	case d <= 0:
		return "-"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < 10*time.Microsecond:
		return fmt.Sprintf("%.1fµs", float64(d)/1e3)
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fµs", float64(d)/1e3)
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/1e6)
	case d < time.Second:
		return fmt.Sprintf("%.0fms", float64(d)/1e6)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 {
		return "-" + commas(-n)
	}

	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
