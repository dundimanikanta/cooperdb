package loadgen

import (
	"testing"
	"time"
)

// TestBucketHoldsItsValue checks every value lands in a bucket whose range contains it, and no bucket is wider than 12.5%.
func TestBucketHoldsItsValue(t *testing.T) {
	values := []int64{0, 1, 7, 8, 9, 15, 16, 17, 100, 999, 1000, 1023, 1024, 123456, 999999999, 1 << 40}

	for v := int64(1); v < 1<<41; v = v*3/2 + 1 {
		values = append(values, v)
	}

	for _, v := range values {
		i := bucketOf(v)

		if top := bucketTop(i); top < v {
			t.Fatalf("value %d went to bucket %d whose top is %d", v, i, top)
		}

		if i > 0 {
			if below := bucketTop(i - 1); below >= v {
				t.Fatalf("value %d went to bucket %d, but bucket %d already reaches %d", v, i, i-1, below)
			}

			if v >= subBuckets && float64(bucketTop(i)-v) > 0.125*float64(v) {
				t.Fatalf("value %d: bucket top %d is more than 12.5%% above it", v, bucketTop(i))
			}
		}
	}
}

// TestPercentilesOfAKnownSpread records 1..10000 µs once each and checks p50, p99 and max.
func TestPercentilesOfAKnownSpread(t *testing.T) {
	var h Histogram
	for us := 1; us <= 10000; us++ {
		h.Record(time.Duration(us) * time.Microsecond)
	}

	var s Snapshot
	h.DrainInto(&s)

	if s.Count() != 10000 {
		t.Fatalf("Count = %d, want 10000", s.Count())
	}

	check := func(name string, got, want time.Duration) {
		t.Helper()
		if got < want || float64(got-want) > 0.125*float64(want) {
			t.Errorf("%s = %v, want %v to %v", name, got, want, time.Duration(1.125*float64(want)))
		}
	}

	check("p50", s.Percentile(50), 5000*time.Microsecond)
	check("p99", s.Percentile(99), 9900*time.Microsecond)

	if s.Max() != 10000*time.Microsecond {
		t.Errorf("Max = %v, want exactly 10ms", s.Max())
	}
}

// TestDrainEmptiesTheHistogram checks a drained histogram starts the next window from zero.
func TestDrainEmptiesTheHistogram(t *testing.T) {
	var h Histogram
	h.Record(time.Millisecond)

	var first, second Snapshot
	h.DrainInto(&first)
	h.DrainInto(&second)

	if first.Count() != 1 || second.Count() != 0 {
		t.Errorf("counts after two drains = %d, %d; want 1, 0", first.Count(), second.Count())
	}

	if second.Max() != 0 {
		t.Errorf("max after the second drain = %v, want 0", second.Max())
	}
}
