package multiraft

import (
	"testing"

	"go.etcd.io/raft/v3"
)

func TestManagerRoutesKeysToIndependentGroups(t *testing.T) {
	manager := NewManager()
	left, err := NewGroup(GroupConfig{Descriptor: RangeDescriptor{ID: 1, StartKey: "", EndKey: "m"}, NodeID: 1, Peers: []raft.Peer{{ID: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewGroup(GroupConfig{Descriptor: RangeDescriptor{ID: 2, StartKey: "m", EndKey: ""}, NodeID: 1, Peers: []raft.Peer{{ID: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	if err := manager.Add(left); err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(right); err != nil {
		t.Fatal(err)
	}
	got, err := manager.Lookup("apple")
	if err != nil || got.Descriptor().ID != 1 {
		t.Fatalf("apple routed to wrong range: group=%v err=%v", got, err)
	}
	got, err = manager.Lookup("zebra")
	if err != nil || got.Descriptor().ID != 2 {
		t.Fatalf("zebra routed to wrong range: group=%v err=%v", got, err)
	}
	if left.node == right.node {
		t.Fatal("ranges must own independent raft nodes")
	}
}

func TestRangeDescriptorUsesHalfOpenIntervals(t *testing.T) {
	r := RangeDescriptor{ID: 7, StartKey: "a", EndKey: "m"}
	if !r.Contains("a") || !r.Contains("hello") || r.Contains("m") || r.Contains("z") {
		t.Fatal("range must use [start, end) semantics")
	}
}
