// Command kvctl is a small CLI for the KV store.
//
//	kvctl [--addrs kv1:7000,kv2:7000,kv3:7000] put <key> <value>
//	kvctl get <key> | del <key> | status | leader <range-id> | wait-ready
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/client"
)

func main() {
	addrs := flag.String("addrs", envOr("KV_ADDRS", "localhost:7001,localhost:7002,localhost:7003"), "comma-separated node addresses ordered by node id")
	timeout := flag.Duration("timeout", 5*time.Second, "overall command timeout")
	httpURL := flag.String("http-url", envOr("KV_HTTP_URL", ""), "use the metadata router's HTTP API at this base URL for get/put")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	list := strings.Split(*addrs, ",")
	c, err := client.New(list)
	check(err)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	_ = c.Refresh(ctx)

	if *httpURL != "" {
		h := client.NewHTTP(*httpURL)
		switch args[0] {
		case "put":
			need(args, 3)
			check(h.Put(ctx, []byte(args[1]), []byte(args[2])))
			fmt.Println("OK")
		case "get":
			need(args, 2)
			v, found, err := h.Get(ctx, []byte(args[1]))
			check(err)
			if !found {
				fmt.Println("(not found)")
				os.Exit(2)
			}
			fmt.Println(string(v))
		default:
			check(fmt.Errorf("only get and put are available with --http-url"))
		}
		return
	}

	switch args[0] {
	case "put":
		need(args, 3)
		check(c.Put(ctx, []byte(args[1]), []byte(args[2])))
		fmt.Println("OK")
	case "get":
		need(args, 2)
		v, found, err := c.Get(ctx, []byte(args[1]))
		check(err)
		if !found {
			fmt.Println("(not found)")
			os.Exit(2)
		}
		fmt.Println(string(v))
	case "del":
		need(args, 2)
		check(c.Delete(ctx, []byte(args[1])))
		fmt.Println("OK")
	case "status":
		out := map[string]any{}
		for _, a := range list {
			st, err := c.Status(ctx, a)
			if err != nil {
				out[a] = map[string]string{"error": err.Error()}
				continue
			}
			out[a] = st
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
	case "leader":
		// Prints the node id currently leading the range (0 = none).
		need(args, 2)
		rid, err := strconv.ParseUint(args[1], 10, 64)
		check(err)
		fmt.Println(c.Leaders(ctx)[rid])
	case "leaders":
		b, _ := json.Marshal(c.Leaders(ctx))
		fmt.Println(string(b))
	case "failover-probe":
		// Watches one range: waits until its current leader stops answering,
		// then measures time to a new elected leader and to the first
		// successful write. Run it, then crash/isolate the leader. Prints JSON.
		need(args, 2)
		rid, err := strconv.ParseUint(args[1], 10, 64)
		check(err)
		failoverProbe(ctx, c, list, rid)
	case "wait-ready":
		// Blocks until every range has a leader and a write succeeds.
		for ctx.Err() == nil {
			_ = c.Refresh(ctx)
			ls := c.Leaders(ctx)
			st, err := c.Status(ctx, list[0])
			if err == nil && len(ls) == len(st.Ranges) && len(ls) > 0 {
				pctx, pcancel := context.WithTimeout(ctx, time.Second)
				err = c.Put(pctx, []byte("__ready"), []byte("1"))
				pcancel()
				if err == nil {
					fmt.Printf("ready: leaders=%v\n", ls)
					return
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		fmt.Fprintln(os.Stderr, "cluster not ready before timeout")
		os.Exit(1)
	default:
		usage()
	}
}

func failoverProbe(ctx context.Context, c *client.Client, list []string, rid uint64) {
	old := c.Leaders(ctx)[rid]
	if old == 0 {
		check(fmt.Errorf("range %d has no leader", rid))
	}
	oldAddr := list[old-1]
	// Phase 1: wait for the old leader to disappear (crash) or step down /
	// lose contact (partition: it stops being leader or its peers elect anew).
	var t0 time.Time
	for ctx.Err() == nil {
		st, err := c.Status(ctx, oldAddr)
		gone := err != nil
		if !gone {
			for _, r := range st.Ranges {
				if r.Range.RangeId == rid && r.RaftState != "StateLeader" {
					gone = true
				}
			}
		}
		if !gone {
			// Partitioned leaders may still believe they lead; check whether
			// any other node already follows a different leader.
			for id, a := range list {
				if uint64(id+1) == old {
					continue
				}
				if st2, err := c.Status(ctx, a); err == nil {
					for _, r := range st2.Ranges {
						if r.Range.RangeId == rid && r.LeaderNodeId != old {
							gone = true
						}
					}
				}
			}
		}
		if gone {
			t0 = time.Now()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "probe: leader %d lost at %s\n", old, t0.Format("15:04:05.000"))
	// Phase 2: a different node reports itself leader.
	var newLeader uint64
	var t1 time.Time
	for ctx.Err() == nil && newLeader == 0 {
		for id, a := range list {
			if uint64(id+1) == old {
				continue
			}
			if st, err := c.Status(ctx, a); err == nil {
				for _, r := range st.Ranges {
					if r.Range.RangeId == rid && r.RaftState == "StateLeader" {
						newLeader, t1 = uint64(id+1), time.Now()
					}
				}
			}
		}
		if newLeader == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	fmt.Fprintf(os.Stderr, "probe: new leader %d at %s\n", newLeader, t1.Format("15:04:05.000"))
	// Phase 3: first successful write to a key in this range.
	key := []byte(fmt.Sprintf("failover-probe-%d", time.Now().UnixNano()))
	st, err := c.Status(ctx, list[newLeader-1])
	check(err)
	for _, r := range st.Ranges {
		if r.Range.RangeId == rid {
			key = append(append([]byte{}, r.Range.StartKey...), key...)
		}
	}
	_ = c.Refresh(ctx)
	for ctx.Err() == nil {
		wctx, cancel := context.WithTimeout(ctx, time.Second)
		err = c.Put(wctx, key, []byte("1"))
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "probe: write failed: %v\n", err)
		}
		if err == nil {
			break
		}
	}
	check(ctx.Err())
	t2 := time.Now()
	out, _ := json.Marshal(map[string]any{
		"range": rid, "old_leader": old, "new_leader": newLeader,
		"election_ms": t1.Sub(t0).Milliseconds(), "first_write_ms": t2.Sub(t0).Milliseconds(),
		"t_detect_unix_ms": t0.UnixMilli(), "t_leader_unix_ms": t1.UnixMilli(), "t_write_unix_ms": t2.UnixMilli(),
	})
	fmt.Println(string(out))
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvctl [--addrs a,b,c] put <k> <v> | get <k> | del <k> | status | leader <range> | leaders | wait-ready | failover-probe <range>")
	os.Exit(64)
}
