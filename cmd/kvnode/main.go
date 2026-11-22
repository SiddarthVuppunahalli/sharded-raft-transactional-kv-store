// Command kvnode runs one storage node of the sharded Raft KV store.
//
//	kvnode --id 1 --listen :7000 \
//	       --peers 1=kv1:7000,2=kv2:7000,3=kv3:7000 --splits m
//
// Every flag can also be set through an environment variable (KV_ID,
// KV_LISTEN, KV_PEERS, KV_SPLITS, KV_REQUEST_TIMEOUT), which is what the
// Docker Compose deployment uses.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/multiraft"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/server"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	id := flag.Uint64("id", mustUint(env("KV_ID", "1")), "node id (1-based)")
	listen := flag.String("listen", env("KV_LISTEN", ":7000"), "listen address")
	peers := flag.String("peers", env("KV_PEERS", "1=localhost:7000"), "comma-separated id=host:port for all nodes, including this one")
	splits := flag.String("splits", env("KV_SPLITS", "m"), "comma-separated split keys; N splits create N+1 ranges")
	timeout := flag.Duration("request-timeout", mustDur(env("KV_REQUEST_TIMEOUT", "2s")), "per-request raft commit timeout")
	flag.Parse()

	peerMap, err := parsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	if _, ok := peerMap[*id]; !ok {
		log.Fatalf("node id %d is not in --peers", *id)
	}
	node, err := server.NewNode(server.Config{
		NodeID:         *id,
		ListenAddr:     *listen,
		Peers:          peerMap,
		Ranges:         RangesFromSplits(*splits),
		RequestTimeout: *timeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := node.Serve(ctx); err != nil {
		log.Fatal(err)
	}
}

// RangesFromSplits turns split keys into contiguous half-open ranges with
// IDs 1..N+1 covering the whole keyspace.
func RangesFromSplits(splits string) []multiraft.RangeDescriptor {
	var keys []string
	for _, s := range strings.Split(splits, ",") {
		if s = strings.TrimSpace(s); s != "" {
			keys = append(keys, s)
		}
	}
	sort.Strings(keys)
	out := make([]multiraft.RangeDescriptor, 0, len(keys)+1)
	start := ""
	for i, k := range keys {
		out = append(out, multiraft.RangeDescriptor{ID: uint64(i + 1), StartKey: start, EndKey: k})
		start = k
	}
	return append(out, multiraft.RangeDescriptor{ID: uint64(len(keys) + 1), StartKey: start})
}

func parsePeers(s string) (map[uint64]string, error) {
	out := map[uint64]string{}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("bad peer id %q", kv[0])
		}
		out[id] = kv[1]
	}
	return out, nil
}

func mustUint(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Fatalf("bad integer %q", s)
	}
	return v
}

func mustDur(s string) time.Duration {
	v, err := time.ParseDuration(s)
	if err != nil {
		log.Fatalf("bad duration %q", s)
	}
	return v
}
