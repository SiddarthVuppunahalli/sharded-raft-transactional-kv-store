package multiraft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

func TestSingleGroupElectsLeaderReplicatesAndReadsWrites(t *testing.T) {
	transport := NewMemoryTransport()
	const nodeCount = 3
	groups := make([]*Group, 0, nodeCount)
	contexts := make([]context.CancelFunc, 0, nodeCount)

	for nodeID := uint64(1); nodeID <= nodeCount; nodeID++ {
		storage, err := OpenPebbleStorage(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = storage.Close() })
		group, err := NewGroup(GroupConfig{
			Descriptor: RangeDescriptor{ID: 1, StartKey: "", EndKey: ""},
			NodeID:     nodeID,
			Peers:      []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}},
			Storage:    storage,
			Transport:  transport,
		})
		if err != nil {
			t.Fatal(err)
		}
		groups = append(groups, group)
	}

	for _, group := range groups {
		ctx, cancel := context.WithCancel(context.Background())
		contexts = append(contexts, cancel)
		go func(group *Group, ctx context.Context) {
			if err := group.Run(ctx); err != nil && err != context.Canceled {
				t.Errorf("group %d stopped: %v", group.NodeID(), err)
			}
		}(group, ctx)
	}
	t.Cleanup(func() {
		for _, cancel := range contexts {
			cancel()
		}
	})

	leader := waitForLeader(t, groups)
	if err := leader.Propose(context.Background(), Command{Op: "put", Key: "apple", Value: []byte("red")}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allReplicated := true
		for _, group := range groups {
			value, found, err := group.Get("apple")
			if err != nil || !found || string(value) != "red" {
				allReplicated = false
				break
			}
		}
		if allReplicated {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for committed write to reach all three replicas")
}

func waitForLeader(t *testing.T, groups []*Group) *Group {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leader *Group
		for _, group := range groups {
			if group.IsLeader() {
				if leader != nil {
					t.Fatalf("multiple leaders observed: %d and %d", leader.NodeID(), group.NodeID())
				}
				leader = group
			}
		}
		if leader != nil {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	ids := make([]string, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, fmt.Sprintf("node %d leader %d", group.NodeID(), group.LeaderID()))
	}
	t.Fatalf("timed out waiting for leader: %v", ids)
	return nil
}
