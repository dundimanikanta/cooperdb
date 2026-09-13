package cooperdb

import (
	"fmt"
	"testing"
)

// benchmarkPut runs b.N writes against a database opened with the given sync
// policy. Keys are built before the timer starts so the measurement covers Put
// and nothing else.
//
// Run these with a fixed iteration count so the three policies are comparable:
//
//	go test -bench=. -benchtime=10000x -run=^$ ./...
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
