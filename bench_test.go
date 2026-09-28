package cooperdb

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// benchmarkPut runs b.N writes against a database opened with the given sync
// policy. Keys are built before the timer starts so the measurement covers Put
// and nothing else.
//
// A fixed count for the fsync-bound policies, 2-second runs for SyncNever, which
// is too fast for 10,000 iterations to get past warm-up:
//
//	go test -bench='PutSyncEveryN|PutSyncAlways' -benchtime=10000x -count=5 -run=^$ ./...
//	go test -bench='PutSyncNever|BenchmarkGet' -benchtime=2s -count=6 -run=^$ ./...
//
// ops/sec is 1e9 divided by the reported ns/op.
func benchmarkPut(b *testing.B, valueSize int, opts ...Option) {
	db, err := Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("user:%d", i))
	}

	value := make([]byte, valueSize)

	// setup is done; everything after this is what gets measured
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = db.Put(keys[i], value)
		if err != nil {
			// StopTimer so the failure path cannot skew the numbers
			b.StopTimer()
			b.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// the file is closed outside the measured region, since Close syncs and
	// would otherwise charge one flush to whichever iteration ran last
	b.StopTimer()

	err = db.Close()
	if err != nil {
		b.Fatalf("Close failed: %v", err)
	}
}

// BenchmarkPutSyncNever measures the default: no flush on the write path, the
// kernel writes back when it chooses.
func BenchmarkPutSyncNever(b *testing.B) {
	benchmarkPut(b, 100, WithSyncNever())
}

// BenchmarkPutSyncEveryN measures one flush per hundred writes.
func BenchmarkPutSyncEveryN(b *testing.B) {
	benchmarkPut(b, 100, WithSyncEveryN(100))
}

// BenchmarkPutSyncAlways measures a flush on every write. Expect this to be
// slower by orders of magnitude — each write waits on the hardware.
func BenchmarkPutSyncAlways(b *testing.B) {
	benchmarkPut(b, 100, WithSyncAlways())
}

// BenchmarkGet measures the read path, which the sync policy does not touch:
// one keydir lookup plus one positional read.
func BenchmarkGet(b *testing.B) {
	db, err := Open(b.TempDir())
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// a fixed working set, so the benchmark does not also measure file growth
	const count = 1000

	keys := make([][]byte, count)
	value := make([]byte, 100)

	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("user:%d", i))

		err = db.Put(keys[i], value)
		if err != nil {
			b.Fatalf("Put %d failed: %v", i, err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = db.Get(keys[i%count])
		if err != nil {
			b.StopTimer()
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkPutWriters runs Put from 1, 10 and 100 goroutines at once, under SyncNever and SyncAlways.
//
//	go test -bench=PutWriters -benchtime=2s -run=^$ ./...
func BenchmarkPutWriters(b *testing.B) {
	policies := []struct {
		name string
		opt  Option
	}{
		{"SyncNever", WithSyncNever()},
		{"SyncAlways", WithSyncAlways()},
	}

	for _, policy := range policies {
		for _, writers := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("%s/writers=%d", policy.name, writers), func(b *testing.B) {
				benchmarkPutWriters(b, writers, policy.opt)
			})
		}
	}
}

// benchmarkPutWriters splits b.N writes across writers goroutines and reports
// total writes/sec plus the p50 and p99 of each Put, waiting for the lock included.
func benchmarkPutWriters(b *testing.B, writers int, opts ...Option) {
	db, err := Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// a bounded keyspace, overwritten, so a long run does not also measure the keydir growing
	const keySpace = 10000

	keys := make([][]byte, keySpace)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("user:%d", i))
	}

	value := make([]byte, 100)

	// one slice per writer, so no two goroutines ever write the same one
	latencies := make([][]time.Duration, writers)

	var wg sync.WaitGroup

	b.ResetTimer()
	start := time.Now()

	for w := 0; w < writers; w++ {
		// b.N shared out as evenly as possible
		n := b.N / writers
		if w < b.N%writers {
			n++
		}

		latencies[w] = make([]time.Duration, 0, n)

		wg.Add(1)
		go func(w, n int) {
			defer wg.Done()

			for i := 0; i < n; i++ {
				key := keys[(i*writers+w)%keySpace]

				began := time.Now()
				if err := db.Put(key, value); err != nil {
					b.Errorf("Put failed: %v", err)
					return
				}
				latencies[w] = append(latencies[w], time.Since(began))
			}
		}(w, n)
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	all := make([]time.Duration, 0, b.N)
	for _, l := range latencies {
		all = append(all, l...)
	}
	slices.Sort(all)

	b.ReportMetric(float64(len(all))/elapsed.Seconds(), "writes/s")
	b.ReportMetric(float64(percentile(all, 50).Nanoseconds())/1e3, "p50-µs")
	b.ReportMetric(float64(percentile(all, 99).Nanoseconds())/1e3, "p99-µs")

	// ns/op would be elapsed/b.N, which is throughput again, not the latency of one Put
	b.ReportMetric(0, "ns/op")

	if err := db.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}
}

// percentile returns the nearest-rank p-th percentile of sorted.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}

	return sorted[rank-1]
}
