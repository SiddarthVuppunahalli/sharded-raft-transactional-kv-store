package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/client"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/multiraft"
)

func freeAddrs(t *testing.T, n int) []string {
	out := make([]string, n)
	for i := range out {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		out[i] = l.Addr().String()
		l.Close()
	}
	return out
}

type testCluster struct {
	addrs   []string
	cancels map[uint64]context.CancelFunc
}

func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	addrs := freeAddrs(t, n)
	peers := map[uint64]string{}
	for i, a := range addrs {
		peers[uint64(i+1)] = a
	}
	ranges := []multiraft.RangeDescriptor{{ID: 1, StartKey: "", EndKey: "m"}, {ID: 2, StartKey: "m"}}
	tc := &testCluster{addrs: addrs, cancels: map[uint64]context.CancelFunc{}}
	for id := range peers {
		node, err := NewNode(Config{NodeID: id, ListenAddr: peers[id], Peers: peers, Ranges: ranges})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		tc.cancels[id] = cancel
		go func() { _ = node.Serve(ctx) }()
	}
	t.Cleanup(func() {
		for _, c := range tc.cancels {
			c()
		}
	})
	return tc
}

func waitLeaders(t *testing.T, c *client.Client, ranges int) map[uint64]uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ls := c.Leaders(context.Background())
		if len(ls) == ranges {
			return ls
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("ranges did not elect leaders")
	return nil
}

func TestThreeNodeClusterReplicatesAndFailsOver(t *testing.T) {
	tc := startCluster(t, 3)
	c, err := client.New(tc.addrs)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	leaders := waitLeaders(t, c, 2)
	_ = c.Refresh(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("%c-key-%d", 'a'+i%26, i)
		if err := c.Put(ctx, []byte(k), []byte(fmt.Sprint(i))); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	v, found, err := c.Get(ctx, []byte("c-key-2"))
	if err != nil || !found || string(v) != "2" {
		t.Fatalf("get c-key-2 = %q %v %v", v, found, err)
	}

	// Every replica (not only the leader) must serve the committed value,
	// since reads go through the Raft log.
	for _, a := range tc.addrs {
		single, _ := client.New([]string{a})
		v, found, err := single.Get(ctx, []byte("t-key-19"))
		single.Close()
		if err != nil || !found || string(v) != "19" {
			t.Fatalf("replica %s: get t-key-19 = %q %v %v", a, v, found, err)
		}
	}

	// Kill the leader of range 1; the survivors must elect a new one and
	// keep accepting writes, and old data must still be readable.
	old := leaders[1]
	tc.cancels[old]()
	start := time.Now()
	if err := c.Put(ctx, []byte("a-after-failover"), []byte("ok")); err != nil {
		t.Fatalf("write after leader crash: %v", err)
	}
	t.Logf("range 1 leader %d stopped; first write after failover took %v", old, time.Since(start))
	v, found, err = c.Get(ctx, []byte("a-key-0"))
	if err != nil || !found || string(v) != "0" {
		t.Fatalf("get a-key-0 after failover = %q %v %v", v, found, err)
	}
	newLeaders := c.Leaders(ctx)
	if newLeaders[1] == old || newLeaders[1] == 0 {
		t.Fatalf("range 1 leader after failover = %d (old %d)", newLeaders[1], old)
	}
}
