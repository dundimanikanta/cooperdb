// Command loadtest-max drives cooperdb as hard as its clients can go, to find
// the maximum throughput.
//
// Its latencies are optimistic. Each client waits for a response before sending
// the next request, so while the database is stalled a client stops sending, and
// the stall is recorded once per client instead of once per request that would
// have arrived during it. That is coordinated omission; loadtest-fixed measures
// latency without it.
//
//	go run ./cmd/loadtest-max -sync never
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dundimanikanta/cooperdb/internal/loadgen"
)

func main() {
	cfg := loadgen.DefaultConfig("max")
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	err := loadgen.Run(cfg, func(c *loadgen.Client) error {
		for !c.Stopped() {
			op, key := c.Next()

			// measured from the moment the request is sent
			if err := c.Do(op, key, time.Now()); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest-max:", err)
		os.Exit(1)
	}
}
