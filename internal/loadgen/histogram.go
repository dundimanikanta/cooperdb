package loadgen

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Each power of two is split into 8 buckets, so a bucket's top is at most 12.5%
// above any value counted in it.
const (
	subBits    = 3
	subBuckets = 1 << subBits
	maxExp     = 42 // 2^42 ns is about 73 minutes
	numBuckets = subBuckets + (maxExp-subBits+1)*subBuckets
)

// Histogram counts latencies into fixed log-linear buckets. One client records
// into it while the reporter drains it, so every counter is atomic.
type Histogram struct {
	counts [numBuckets]atomic.Uint64
	max    atomic.Int64
}

func bucketOf(ns int64) int {
	if ns < subBuckets {
		if ns < 0 {
			return 0
		}
		return int(ns)
	}

	exp := bits.Len64(uint64(ns)) - 1
	if exp > maxExp {
		return numBuckets - 1
	}

	sub := int(ns>>(exp-subBits)) - subBuckets
	return subBuckets + (exp-subBits)*subBuckets + sub
}

// bucketTop is the largest value bucket i can hold.
func bucketTop(i int) int64 {
	if i < subBuckets {
		return int64(i)
	}

	exp := (i-subBuckets)/subBuckets + subBits
	sub := (i - subBuckets) % subBuckets
	return int64(subBuckets+sub+1)<<(exp-subBits) - 1
}

func (h *Histogram) Record(d time.Duration) {
	ns := int64(d)
	h.counts[bucketOf(ns)].Add(1)

	for {
		cur := h.max.Load()
		if ns <= cur || h.max.CompareAndSwap(cur, ns) {
			return
		}
	}
}

// DrainInto adds h's counts to s and zeroes them, so the next window starts empty.
func (h *Histogram) DrainInto(s *Snapshot) {
	for i := range h.counts {
		if n := h.counts[i].Swap(0); n != 0 {
			s.counts[i] += n
		}
	}

	if m := h.max.Swap(0); m > s.max {
		s.max = m
	}
}

// Snapshot is a plain copy of drained counts, read by one goroutine only.
type Snapshot struct {
	counts [numBuckets]uint64
	max    int64
}

func (s *Snapshot) Add(o *Snapshot) {
	for i := range s.counts {
		s.counts[i] += o.counts[i]
	}

	if o.max > s.max {
		s.max = o.max
	}
}

func (s *Snapshot) Count() uint64 {
	var n uint64
	for _, c := range s.counts {
		n += c
	}
	return n
}

func (s *Snapshot) Max() time.Duration {
	return time.Duration(s.max)
}

// Percentile returns the top of the bucket holding the p-th percentile, capped at
// the largest value actually recorded.
func (s *Snapshot) Percentile(p float64) time.Duration {
	total := s.Count()
	if total == 0 {
		return 0
	}

	rank := uint64(math.Ceil(p / 100 * float64(total)))
	if rank < 1 {
		rank = 1
	}

	var cum uint64
	for i, c := range s.counts {
		cum += c
		if cum >= rank {
			return time.Duration(min(bucketTop(i), s.max))
		}
	}

	return time.Duration(s.max)
}
