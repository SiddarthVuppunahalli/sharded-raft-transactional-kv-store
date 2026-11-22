// Command kvbench drives workloads against the cluster and reports
// throughput, latency, availability and correctness.
//
// Workloads:
//
//	kv      mixed reads/writes over --keys keys (throughput / latency)
//	linear  reads/writes with unique values on few keys; the history is
//	        checked for linearizability with porcupine
//	bank    multi-range transfers through the Txn API; an auditor checks
//	        that the total balance never changes (atomicity + isolation)
//
// Results are printed and written as JSON (--out); --summary-md appends a
// row to a Markdown results table.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/client"
)

type options struct {
	addrs       []string
	workload    string
	duration    time.Duration
	concurrency int
	readRatio   float64
	keys        int
	valueSize   int
	opTimeout   time.Duration
	accounts    int
	label       string
	out         string
	summaryMD   string
	checkTime   time.Duration
	visualize   string
}

func main() {
	var o options
	addrs := flag.String("addrs", envOr("KV_ADDRS", "localhost:7001,localhost:7002,localhost:7003"), "node addresses ordered by node id")
	flag.StringVar(&o.workload, "workload", "kv", "kv | linear | bank")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "how long to run")
	flag.IntVar(&o.concurrency, "concurrency", 16, "concurrent client workers")
	flag.Float64Var(&o.readRatio, "read-ratio", 0.5, "fraction of operations that are reads")
	flag.IntVar(&o.keys, "keys", 1000, "number of distinct keys")
	flag.IntVar(&o.valueSize, "value-size", 64, "value size in bytes (kv workload)")
	flag.DurationVar(&o.opTimeout, "op-timeout", 3*time.Second, "per-operation timeout including client retries")
	flag.IntVar(&o.accounts, "accounts", 10, "bank workload: number of accounts")
	flag.StringVar(&o.label, "label", "", "scenario label for reports")
	flag.StringVar(&o.out, "out", "", "write JSON result to this file")
	flag.StringVar(&o.summaryMD, "summary-md", "", "append a Markdown table row to this file")
	flag.DurationVar(&o.checkTime, "check-timeout", 2*time.Minute, "linear workload: porcupine checker timeout")
	flag.StringVar(&o.visualize, "visualize", "", "linear workload: write porcupine HTML visualization here")
	flag.Parse()
	o.addrs = strings.Split(*addrs, ",")
	if o.label == "" {
		o.label = o.workload
	}

	c, err := client.New(o.addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	_ = c.Refresh(ctx)

	var res *Result
	switch o.workload {
	case "kv":
		res = runKV(ctx, c, o)
	case "linear":
		res = runLinear(ctx, c, o)
	case "bank":
		res = runBank(ctx, c, o)
	default:
		log.Fatalf("unknown workload %q", o.workload)
	}
	res.Label = o.label
	res.Workload = o.workload
	res.Concurrency = o.concurrency
	res.print()

	if o.out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(o.out, b, 0o644); err != nil {
			log.Printf("write %s: %v", o.out, err)
		}
	}
	if o.summaryMD != "" {
		appendSummary(o.summaryMD, res)
	}
	if res.Correctness == "VIOLATION" {
		os.Exit(3)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func appendSummary(path string, r *Result) {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("summary: %v", err)
		return
	}
	defer f.Close()
	if os.IsNotExist(statErr) {
		fmt.Fprintln(f, "| Scenario | Workload | Ops/s | OK | Errors | Read p50 / p99 (ms) | Write p50 / p99 (ms) | Max unavailability (ms) | Correctness |")
		fmt.Fprintln(f, "|---|---|---:|---:|---:|---:|---:|---:|---|")
	}
	fmt.Fprintf(f, "| %s | %s | %.0f | %d | %d | %.1f / %.1f | %.1f / %.1f | %.0f | %s |\n",
		r.Label, r.Workload, r.Throughput, r.OK, r.Errors,
		r.Read.P50, r.Read.P99, r.Write.P50, r.Write.P99, r.MaxGapMs, r.Correctness)
}
