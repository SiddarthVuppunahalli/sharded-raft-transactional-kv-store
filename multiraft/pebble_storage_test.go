package multiraft

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestPebbleStoragePersistsRaftStateAndKeyValues(t *testing.T) {
	directory := t.TempDir()
	storage, err := OpenPebbleStorage(directory)
	if err != nil {
		t.Fatal(err)
	}

	if err := storage.SetHardState(raftpb.HardState{Term: 3, Vote: 2, Commit: 4}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Append([]raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Apply(Command{Op: "put", Key: "apple", Value: []byte("red")}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	storage, err = OpenPebbleStorage(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	hardState, _, err := storage.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if hardState.Term != 3 || hardState.Vote != 2 || hardState.Commit != 4 {
		t.Fatalf("unexpected hard state: %+v", hardState)
	}
	entries, err := storage.Entries(1, 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[1].Term != 3 {
		t.Fatalf("unexpected entries: %+v", entries)
	}
	value, ok, err := storage.Get("apple")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "red" {
		t.Fatalf("unexpected value: %q, found=%v", value, ok)
	}
}
