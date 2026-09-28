package cooperdb

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	crashKeys        = 64
	crashRuns        = 20
	crashMaxFileSize = 1024
	crashMergeEvery  = 20

	// what the model holds for a key that must not exist
	absent = "<absent>"
)

func crashKey(i int) string {
	return fmt.Sprintf("key%02d", i)
}

// crashOp is one operation the child announced: a put, a del or a merge.
type crashOp struct {
	kind  string
	key   string
	value string
}

// stateAfter is what op leaves its key holding.
func (op crashOp) stateAfter() string {
	if op.kind == "put" {
		return op.value
	}
	return absent
}

// parseCrashOp reads an op from the fields after "CRASH intent" or "CRASH ack".
func parseCrashOp(fields []string) (crashOp, bool) {
	switch {
	case len(fields) == 1 && fields[0] == "merge":
		return crashOp{kind: "merge"}, true
	case len(fields) == 3 && fields[0] == "put":
		return crashOp{kind: "put", key: fields[1], value: fields[2]}, true
	case len(fields) == 2 && fields[0] == "del":
		return crashOp{kind: "del", key: fields[1]}, true
	}
	return crashOp{}, false
}

// TestReopenWithoutCloseFindsEveryWrite writes, abandons the database without Close, and checks a reopen finds every write.
func TestReopenWithoutCloseFindsEveryWrite(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)

		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// no db.Close() — the pretend crash

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		want := fmt.Sprintf("value%03d", i)

		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", key, err)
		}

		if string(got) != want {
			t.Errorf("Get(%s) = %q, want %q", key, got, want)
		}
	}
}

// TestCrashChild runs only when the parent starts it; in a normal go test it returns at once.
func TestCrashChild(t *testing.T) {
	if os.Getenv("COOPERDB_CRASH_CHILD") != "1" {
		return
	}

	dir := os.Getenv("COOPERDB_CRASH_DIR")
	gen, _ := strconv.Atoi(os.Getenv("COOPERDB_CRASH_GEN"))

	// small files, so rotation happens constantly and merge always has sealed files to work on
	db, err := Open(dir, WithMaxFileSize(crashMaxFileSize))
	if err != nil {
		fmt.Printf("CRASH fail %v\n", err)
		os.Exit(1)
	}

	// a merge takes far longer than a put, so merging every generation would put nearly
	// every kill inside one; one in three keeps kills between writes common too
	merges := gen%3 == 0

	fmt.Println("CRASH ready")

	// forever: the only way this ends is the parent's SIGKILL
	for i := 0; ; i++ {
		if merges && i > 0 && i%crashMergeEvery == 0 {
			fmt.Println("CRASH intent merge")

			if err := db.Merge(); err != nil {
				fmt.Printf("CRASH fail %v\n", err)
				os.Exit(1)
			}

			fmt.Println("CRASH ack merge")
			continue
		}

		key := crashKey(rand.Intn(crashKeys))

		// one in five is a delete, so absence gets promised as often as values do
		if rand.Intn(5) == 0 {
			fmt.Printf("CRASH intent del %s\n", key)

			if err := db.Delete([]byte(key)); err != nil {
				fmt.Printf("CRASH fail %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("CRASH ack del %s\n", key)
			continue
		}

		// unique across every child ever run on this folder, so an old version can never pass for a new one
		value := fmt.Sprintf("%s@g%ds%d", key, gen, i)

		// announced before the write, so the parent knows what may be half-done when the kill lands
		fmt.Printf("CRASH intent put %s %s\n", key, value)

		if err := db.Put([]byte(key), []byte(value)); err != nil {
			fmt.Printf("CRASH fail %v\n", err)
			os.Exit(1)
		}

		// printed only after Put returned nil: this line is the promise
		fmt.Printf("CRASH ack put %s %s\n", key, value)
	}
}

// runCrashGeneration starts a child on dir, SIGKILLs it after delay, and returns
// the operations it acknowledged, in order, plus the one that may have been in flight.
func runCrashGeneration(t *testing.T, dir string, gen int, delay time.Duration) ([]crashOp, *crashOp) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(),
		"COOPERDB_CRASH_CHILD=1",
		"COOPERDB_CRASH_DIR="+dir,
		"COOPERDB_CRASH_GEN="+strconv.Itoa(gen),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe failed: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the child failed: %v", err)
	}
	defer cmd.Process.Kill()

	var lines []string
	ready := make(chan struct{})
	done := make(chan struct{})

	// reads while the child runs, and keeps reading after the kill until nothing is left
	go func() {
		defer close(done)

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			lines = append(lines, line)

			if line == "CRASH ready" {
				close(ready)
			}
		}
	}()

	select {
	case <-ready:
	case <-done:
		t.Fatalf("gen %d: the child ended before it was ready: %v", gen, lines)
	}

	time.Sleep(delay)

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("gen %d: kill failed: %v", gen, err)
	}

	<-done
	cmd.Wait()

	if cmd.ProcessState.Exited() {
		t.Fatalf("gen %d: the child exited on its own instead of being killed: %v", gen, cmd.ProcessState)
	}

	var acked []crashOp
	var inflight *crashOp

	for _, line := range lines {
		fields := strings.Fields(line)

		if len(fields) < 3 || fields[0] != "CRASH" {
			continue
		}

		op, ok := parseCrashOp(fields[2:])
		if !ok {
			continue
		}

		switch fields[1] {
		case "intent":
			inflight = &op
		case "ack":
			acked = append(acked, op)
			inflight = nil
		}
	}

	return acked, inflight
}

// countTornFiles reports how many data files end in bytes a scan cannot read.
func countTornFiles(t *testing.T, dir string) int {
	t.Helper()

	ids, err := dataFileIDs(dir)
	if err != nil {
		t.Fatalf("dataFileIDs failed: %v", err)
	}

	torn := 0

	for _, id := range ids {
		df, err := OpenDataFile(dir, id, dontCreateIfMissing)
		if err != nil {
			t.Fatalf("opening file %d failed: %v", id, err)
		}

		validLen, err := df.Scan(func(int64, *Record) error { return nil })
		df.Close()

		if err != nil {
			t.Fatalf("scanning file %d failed: %v", id, err)
		}

		// df.offset is the file's size when it was opened
		if validLen < df.offset {
			torn++
		}
	}

	return torn
}

// TestAcknowledgedWritesSurviveRepeatedKills SIGKILLs a child that writes, deletes and merges, many times on one directory, and checks every acknowledged operation after each restart.
func TestAcknowledgedWritesSurviveRepeatedKills(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a process per kill; skipped under -short")
	}

	runs := crashRuns
	if s := os.Getenv("COOPERDB_CRASH_RUNS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatalf("COOPERDB_CRASH_RUNS=%q is not a positive number", s)
		}
		runs = n
	}

	dir := t.TempDir()

	// created once and carried across every kill, so each check covers the whole history
	model := make(map[string]string)
	for i := 0; i < crashKeys; i++ {
		model[crashKey(i)] = absent
	}

	totalAcked := 0
	merges := 0
	midMerge := 0
	strays := 0
	torn := 0
	outcomes := make(map[string]int)

	for gen := 0; gen < runs; gen++ {
		delay := time.Duration(5+rand.Intn(56)) * time.Millisecond

		acked, inflight := runCrashGeneration(t, dir, gen, delay)

		for _, op := range acked {
			if op.kind == "merge" {
				merges++
				continue
			}
			model[op.key] = op.stateAfter()
		}
		totalAcked += len(acked)

		if inflight != nil && inflight.kind == "merge" {
			midMerge++
		}

		// counted before Open, which sweeps the strays and trims the damage
		found, err := filepath.Glob(filepath.Join(dir, "*.merge.tmp"))
		if err != nil {
			t.Fatalf("Glob failed: %v", err)
		}
		strays += len(found)
		torn += countTornFiles(t, dir)

		db, err := Open(dir)
		if err != nil {
			t.Fatalf("gen %d: reopen after the kill failed: %v", gen, err)
		}

		outcome := "none"
		present := 0

		for key, want := range model {
			got := absent

			value, err := db.Get([]byte(key))
			switch {
			case err == nil:
				got = string(value)
			case errors.Is(err, ErrKeyNotFound):
			default:
				t.Fatalf("gen %d: Get(%s) failed: %v", gen, key, err)
			}

			// the in-flight operation may or may not have landed; both are correct
			if inflight != nil && key == inflight.key {
				switch {
				case want == inflight.stateAfter():
					outcome = "no visible change"
				case got == inflight.stateAfter():
					outcome = "landed"
					want = got
					// carried forward, or the next generation would expect the old state
					model[key] = got
				default:
					outcome = "did not land"
				}
			}

			if got != want {
				t.Fatalf("gen %d: %s = %q, want %q", gen, key, got, want)
			}

			if got != absent {
				present++
			}
		}

		if db.keyDirectory.Len() != present {
			t.Fatalf("gen %d: keydir holds %d keys, the model %d: a key appeared that nothing wrote", gen, db.keyDirectory.Len(), present)
		}

		outcomes[outcome]++

		// released before the next child opens the same directory
		if err := db.Close(); err != nil {
			t.Fatalf("gen %d: Close failed: %v", gen, err)
		}
	}

	t.Logf("%d kills on one directory, %d acknowledged operations verified, %d merges completed, killed mid-merge %d times, stray .merge.tmp files %d, torn data files %d; in-flight outcomes: %v",
		runs, totalAcked, merges, midMerge, strays, torn, outcomes)
}
