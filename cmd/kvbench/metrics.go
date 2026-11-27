package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// LatencyStats summarises successful operation latencies in milliseconds.
type LatencyStats struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

type Second struct {
	T      int `json:"t"`
	OK     int `json:"ok"`
	Errors int `json:"errors"`
}

type Result struct {
	Label       string         `json:"label"`
	Workload    string         `json:"workload"`
	Concurrency int            `json:"concurrency"`
	DurationSec float64        `json:"duration_sec"`
	OK          int            `json:"ok"`
	Errors      int            `json:"errors"`
	ErrorKinds  map[string]int `json:"error_kinds,omitempty"`
	Throughput  float64        `json:"throughput_ops_per_sec"`
	Read        LatencyStats   `json:"read"`
	Write       LatencyStats   `json:"write"`
	// MaxGapMs is the longest interval with no successful operation
	// completing: the client-visible unavailability window (e.g. failover).
	MaxGapMs    float64  `json:"max_unavailability_ms"`
	MaxGapAtSec float64  `json:"max_unavailability_at_sec"`
	Timeline    []Second `json:"timeline"`
	Correctness string   `json:"correctness"` // OK | VIOLATION | UNKNOWN | n/a | SKIPPED
	Details     string   `json:"details,omitempty"`
}

type sample struct {
	start, end time.Time
	write      bool
	ok         bool
	errKind    string
}

// Recorder collects samples from concurrent workers.
type Recorder struct {
	mu      sync.Mutex
	begin   time.Time
	samples []sample
}

func NewRecorder() *Recorder { return &Recorder{begin: time.Now()} }

func (r *Recorder) Add(s sample) {
	r.mu.Lock()
	r.samples = append(r.samples, s)
	r.mu.Unlock()
}

func (r *Recorder) Result(end time.Time) *Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := &Result{DurationSec: end.Sub(r.begin).Seconds(), ErrorKinds: map[string]int{}, Correctness: "n/a"}
	var reads, writes []float64
	var okEnds []time.Time
	secs := int(res.DurationSec) + 1
	res.Timeline = make([]Second, secs)
	for i := range res.Timeline {
		res.Timeline[i].T = i
	}
	for _, s := range r.samples {
		sec := int(s.end.Sub(r.begin).Seconds())
		if sec >= secs {
			sec = secs - 1
		}
		if s.ok {
			res.OK++
			res.Timeline[sec].OK++
			okEnds = append(okEnds, s.end)
			ms := float64(s.end.Sub(s.start).Microseconds()) / 1000
			if s.write {
				writes = append(writes, ms)
			} else {
				reads = append(reads, ms)
			}
		} else {
			res.Errors++
			res.Timeline[sec].Errors++
			res.ErrorKinds[s.errKind]++
		}
	}
	res.Throughput = float64(res.OK) / res.DurationSec
	res.Read = stats(reads)
	res.Write = stats(writes)

	sort.Slice(okEnds, func(i, j int) bool { return okEnds[i].Before(okEnds[j]) })
	prev := r.begin
	for _, t := range append(okEnds, end) {
		if gap := t.Sub(prev); float64(gap.Milliseconds()) > res.MaxGapMs {
			res.MaxGapMs = float64(gap.Milliseconds())
			res.MaxGapAtSec = prev.Sub(r.begin).Seconds()
		}
		prev = t
	}
	return res
}

func stats(v []float64) LatencyStats {
	if len(v) == 0 {
		return LatencyStats{}
	}
	sort.Float64s(v)
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	pct := func(p float64) float64 { return v[int(p*float64(len(v)-1))] }
	return LatencyStats{Count: len(v), Mean: sum / float64(len(v)), P50: pct(0.50), P95: pct(0.95), P99: pct(0.99), Max: v[len(v)-1]}
}

func (r *Result) print() {
	fmt.Printf("\n=== %s (%s, %d workers, %.1fs) ===\n", r.Label, r.Workload, r.Concurrency, r.DurationSec)
	fmt.Printf("ok=%d errors=%d throughput=%.0f ops/s\n", r.OK, r.Errors, r.Throughput)
	if len(r.ErrorKinds) > 0 {
		fmt.Printf("error kinds: %v\n", r.ErrorKinds)
	}
	fmt.Printf("read  latency ms: p50=%.2f p95=%.2f p99=%.2f max=%.2f (n=%d)\n", r.Read.P50, r.Read.P95, r.Read.P99, r.Read.Max, r.Read.Count)
	fmt.Printf("write latency ms: p50=%.2f p95=%.2f p99=%.2f max=%.2f (n=%d)\n", r.Write.P50, r.Write.P95, r.Write.P99, r.Write.Max, r.Write.Count)
	fmt.Printf("max unavailability: %.0f ms (starting at t=%.1fs)\n", r.MaxGapMs, r.MaxGapAtSec)
	fmt.Printf("correctness: %s %s\n", r.Correctness, r.Details)
	fmt.Print("timeline (ok/err per second): ")
	for _, s := range r.Timeline {
		fmt.Printf("%d/%d ", s.OK, s.Errors)
	}
	fmt.Println()
}
