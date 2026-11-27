package multiraft

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

func TestSingleGroupRestoresStateAfterLocalCrash(t *testing.T) {
	transport := NewMemoryTransport()
	const nodeCount = 3
	groups := make([]*Group, nodeCount)
	storages := make([]*PebbleStorage, nodeCount)
	directories := make([]string, nodeCount)
	cancels := make([]context.CancelFunc, nodeCount)
	dones := make([]<-chan struct{}, nodeCount)

	for nodeID := uint64(1); nodeID <= nodeCount; nodeID++ {
		directories[nodeID-1] = t.TempDir()
		storage, err := OpenPebbleStorage(directories[nodeID-1])
		if err != nil {
			t.Fatal(err)
		}
		storages[nodeID-1] = storage
		groups[nodeID-1], err = NewGroup(GroupConfig{
			Descriptor: RangeDescriptor{ID: 1},
			NodeID:     nodeID,
			Peers:      []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}},
			Storage:    storage,
			Transport:  transport,
		})
		if err != nil {
			t.Fatal(err)
		}
		cancels[nodeID-1], dones[nodeID-1] = runTestGroup(t, groups[nodeID-1])
	}
	t.Cleanup(func() {
		for i, group := range groups {
			if cancels[i] != nil {
				cancels[i]()
			}
			if group != nil {
				group.Stop()
			}
			if storages[i] != nil {
				_ = storages[i].Close()
			}
		}
	})

	leader := waitForLeader(t, groups)
	if err := leader.Propose(context.Background(), Command{Op: "put", Key: "before-crash", Value: []byte("durable")}); err != nil {
		t.Fatal(err)
	}
	waitForValueOnAllGroups(t, groups, "before-crash", "durable")

	// Stop a follower, close its Pebble database, and recreate the local Raft
	// node from the same directory. The remaining two nodes retain a quorum.
	crashed := 0
	for i, group := range groups {
		if group != leader {
			crashed = i
			break
		}
	}
	cancels[crashed]()
	groups[crashed].Stop()
	<-dones[crashed]
	if err := storages[crashed].Close(); err != nil {
		t.Fatal(err)
	}

	restartedStorage, err := OpenPebbleStorage(directories[crashed])
	if err != nil {
		t.Fatal(err)
	}
	storages[crashed] = restartedStorage
	restarted, err := NewGroup(GroupConfig{
		Descriptor: RangeDescriptor{ID: 1},
		NodeID:     uint64(crashed + 1),
		Storage:    restartedStorage,
		Transport:  transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	groups[crashed] = restarted
	cancels[crashed], dones[crashed] = runTestGroup(t, restarted)

	waitForValueOnGroup(t, restarted, "before-crash", "durable")
	leader = waitForLeader(t, groups)
	if err := leader.Propose(context.Background(), Command{Op: "put", Key: "after-crash", Value: []byte("recovered")}); err != nil {
		t.Fatal(err)
	}
	waitForValueOnAllGroups(t, groups, "after-crash", "recovered")
}

func runTestGroup(t *testing.T, group *Group) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := group.Run(ctx); err != nil && err != context.Canceled {
			t.Errorf("group %d stopped: %v", group.NodeID(), err)
		}
	}()
	return cancel, done
}

func waitForValueOnAllGroups(t *testing.T, groups []*Group, key, expected string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allFound := true
		for _, group := range groups {
			value, found, err := group.Get(key)
			if err != nil || !found || string(value) != expected {
				allFound = false
				break
			}
		}
		if allFound {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q=%q on all replicas", key, expected)
}

func waitForValueOnGroup(t *testing.T, group *Group, key, expected string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		value, found, err := group.Get(key)
		if err == nil && found && string(value) == expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for recovered value %q=%q on node %d", key, expected, group.NodeID())
}
