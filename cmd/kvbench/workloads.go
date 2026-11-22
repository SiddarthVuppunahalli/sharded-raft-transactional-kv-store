package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/client"
)

func errKind(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Code().String()
	}
	if err == context.DeadlineExceeded {
		return codes.DeadlineExceeded.String()
	}
	return "Other"
}

// keyName spreads keys over the whole keyspace so every range gets traffic.
func keyName(i int) string {
	prefixes := "abcdefghijklmnopqrstuvwxyz"
	return fmt.Sprintf("%c-key-%06d", prefixes[i%len(prefixes)], i)
}

func runWorkers(ctx context.Context, o options, fn func(ctx context.Context, worker int, rng *rand.Rand)) time.Time {
	ctx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)))
			for ctx.Err() == nil {
				fn(ctx, w, rng)
			}
		}(w)
	}
	wg.Wait()
	return time.Now()
}

// ---------------------------------------------------------------- kv

func runKV(ctx context.Context, c *client.Client, o options) *Result {
	rec := NewRecorder()
	value := make([]byte, o.valueSize)
	for i := range value {
		value[i] = 'x'
	}
	end := runWorkers(ctx, o, func(wctx context.Context, w int, rng *rand.Rand) {
		key := []byte(keyName(rng.Intn(o.keys)))
		write := rng.Float64() >= o.readRatio
		octx, cancel := context.WithTimeout(context.Background(), o.opTimeout)
		defer cancel()
		start := time.Now()
		var err error
		if write {
			err = c.Put(octx, key, value)
		} else {
			_, _, err = c.Get(octx, key)
		}
		s := sample{start: start, end: time.Now(), write: write, ok: err == nil}
		if err != nil {
			s.errKind = errKind(err)
		}
		rec.Add(s)
	})
	return rec.Result(end)
}

// ---------------------------------------------------------------- linear

type regInput struct {
	Write bool
	Key   string
	Value string
}

type regOutput struct {
	Value string
	Found bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(regInput).Key
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return regOutput{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(regOutput)
		in := input.(regInput)
		if in.Write {
			return true, regOutput{Value: in.Value, Found: true}
		}
		out := output.(regOutput)
		return out == st, st
	},
	Equal: func(a, b interface{}) bool { return a == b },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(regInput)
		if in.Write {
			return fmt.Sprintf("put(%s, %s)", in.Key, in.Value)
		}
		out := output.(regOutput)
		if !out.Found {
			return fmt.Sprintf("get(%s) -> <none>", in.Key)
		}
		return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
	},
}

func runLinear(ctx context.Context, c *client.Client, o options) *Result {
	rec := NewRecorder()
	base := time.Now()
	var mu sync.Mutex
	var history []porcupine.Operation
	var seq atomic.Int64
	// Use a dedicated prefix so values from earlier runs do not leak in.
	runID := strconv.FormatInt(time.Now().UnixNano()%1_000_000, 36)

	end := runWorkers(ctx, o, func(wctx context.Context, w int, rng *rand.Rand) {
		key := fmt.Sprintf("%s-lin-%s-%d", keyName(rng.Intn(o.keys))[:1], runID, rng.Intn(o.keys))
		write := rng.Float64() >= o.readRatio
		octx, cancel := context.WithTimeout(context.Background(), o.opTimeout)
		defer cancel()
		start := time.Now()
		call := start.Sub(base).Nanoseconds()
		var op porcupine.Operation
		var err error
		if write {
			v := fmt.Sprintf("w%d-%d", w, seq.Add(1))
			err = c.Put(octx, []byte(key), []byte(v))
			ret := time.Since(base).Nanoseconds()
			if err != nil {
				// Unknown outcome: the write may still commit later, so it is
				// concurrent with everything after its invocation.
				ret = math.MaxInt64
			}
			op = porcupine.Operation{ClientId: w, Input: regInput{Write: true, Key: key, Value: v}, Call: call, Output: regOutput{}, Return: ret}
		} else {
			var val []byte
			var found bool
			val, found, err = c.Get(octx, []byte(key))
			if err == nil {
				op = porcupine.Operation{ClientId: w, Input: regInput{Key: key}, Call: call, Output: regOutput{Value: string(val), Found: found}, Return: time.Since(base).Nanoseconds()}
			}
		}
		if write || err == nil {
			mu.Lock()
			history = append(history, op)
			mu.Unlock()
		}
		s := sample{start: start, end: time.Now(), write: write, ok: err == nil}
		if err != nil {
			s.errKind = errKind(err)
		}
		rec.Add(s)
	})
	res := rec.Result(end)

	result, info := porcupine.CheckOperationsVerbose(kvModel, history, o.checkTime)
	switch result {
	case porcupine.Ok:
		res.Correctness = "OK"
	case porcupine.Illegal:
		res.Correctness = "VIOLATION"
	default:
		res.Correctness = "UNKNOWN"
	}
	res.Details = fmt.Sprintf("linearizability check over %d operations on %d keys", len(history), o.keys)
	if o.visualize != "" {
		_ = porcupine.VisualizePath(kvModel, info, o.visualize)
	}
	return res
}

// ---------------------------------------------------------------- bank

const initialBalance = 100

func accountKey(i int) string {
	// Alternate halves of the keyspace so transfers cross ranges.
	if i%2 == 0 {
		return fmt.Sprintf("b-acct-%03d", i)
	}
	return fmt.Sprintf("x-acct-%03d", i)
}

func runBank(ctx context.Context, c *client.Client, o options) *Result {
	rec := NewRecorder()
	res := &Result{}
	n := o.accounts
	init := &kvpb.TxnRequest{}
	for i := 0; i < n; i++ {
		init.Ops = append(init.Ops, &kvpb.TxnOp{Type: kvpb.TxnOp_PUT, Key: []byte(accountKey(i)), Value: []byte(strconv.Itoa(initialBalance))})
	}
	ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err := c.Txn(ictx, init)
	cancel()
	if status.Code(err) == codes.Unimplemented {
		res.Correctness = "SKIPPED"
		res.Details = "transaction layer (Txn RPC) not registered on the nodes yet"
		res.Timeline = []Second{}
		return res
	}
	if err != nil {
		res.Correctness = "UNKNOWN"
		res.Details = "could not initialise accounts: " + err.Error()
		return res
	}

	readAll := &kvpb.TxnRequest{}
	for i := 0; i < n; i++ {
		readAll.Ops = append(readAll.Ops, &kvpb.TxnOp{Type: kvpb.TxnOp_GET, Key: []byte(accountKey(i))})
	}
	var audits, violations, aborted atomic.Int64
	var firstViolation atomic.Value
	audit := func(actx context.Context) {
		r, err := c.Txn(actx, readAll)
		if err != nil || r.Status != kvpb.TxnStatus_COMMITTED {
			return
		}
		sum := 0
		for _, op := range r.Results {
			v, _ := strconv.Atoi(string(op.Value))
			sum += v
		}
		audits.Add(1)
		if sum != n*initialBalance {
			violations.Add(1)
			firstViolation.CompareAndSwap(nil, fmt.Sprintf("total=%d want=%d", sum, n*initialBalance))
		}
	}

	auditCtx, stopAudit := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-auditCtx.Done():
				return
			case <-t.C:
				actx, cancel := context.WithTimeout(auditCtx, o.opTimeout)
				audit(actx)
				cancel()
			}
		}
	}()

	end := runWorkers(ctx, o, func(wctx context.Context, w int, rng *rand.Rand) {
		i, j := rng.Intn(n), rng.Intn(n)
		if i == j {
			return
		}
		octx, cancel := context.WithTimeout(context.Background(), o.opTimeout)
		defer cancel()
		start := time.Now()
		ok, err := transfer(octx, c, accountKey(i), accountKey(j), 1+rng.Intn(10))
		if err == nil && !ok {
			aborted.Add(1)
		}
		s := sample{start: start, end: time.Now(), write: true, ok: err == nil}
		if err != nil {
			s.errKind = errKind(err)
		}
		rec.Add(s)
	})
	stopAudit()
	fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	audit(fctx) // final audit after all transfers settle
	cancel()

	r := rec.Result(end)
	r.Correctness = "OK"
	if violations.Load() > 0 {
		r.Correctness = "VIOLATION"
	} else if audits.Load() == 0 {
		r.Correctness = "UNKNOWN"
	}
	r.Details = fmt.Sprintf("%d audits, %d violations, %d aborted transfers", audits.Load(), violations.Load(), aborted.Load())
	if v := firstViolation.Load(); v != nil {
		r.Details += ", first: " + v.(string)
	}
	return r
}

// transfer moves amount from a to b using read-then-guarded-write
// transactions. It returns ok=false if the guarded write aborted.
func transfer(ctx context.Context, c *client.Client, a, b string, amount int) (bool, error) {
	read, err := c.Txn(ctx, &kvpb.TxnRequest{Ops: []*kvpb.TxnOp{
		{Type: kvpb.TxnOp_GET, Key: []byte(a)}, {Type: kvpb.TxnOp_GET, Key: []byte(b)},
	}})
	if err != nil {
		return false, err
	}
	if read.Status != kvpb.TxnStatus_COMMITTED || len(read.Results) != 2 {
		return false, nil
	}
	va, vb := read.Results[0].Value, read.Results[1].Value
	ba, _ := strconv.Atoi(string(va))
	bb, _ := strconv.Atoi(string(vb))
	if ba < amount {
		return true, nil // insufficient funds, nothing to do
	}
	w, err := c.Txn(ctx, &kvpb.TxnRequest{
		Compares: []*kvpb.TxnCompare{
			{Key: []byte(a), ExpectFound: true, ExpectedValue: va},
			{Key: []byte(b), ExpectFound: true, ExpectedValue: vb},
		},
		Ops: []*kvpb.TxnOp{
			{Type: kvpb.TxnOp_PUT, Key: []byte(a), Value: []byte(strconv.Itoa(ba - amount))},
			{Type: kvpb.TxnOp_PUT, Key: []byte(b), Value: []byte(strconv.Itoa(bb + amount))},
		},
	})
	if err != nil {
		return false, err
	}
	return w.Status == kvpb.TxnStatus_COMMITTED, nil
}
