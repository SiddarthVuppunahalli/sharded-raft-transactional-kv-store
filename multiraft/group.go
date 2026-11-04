package multiraft

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Transport delivers Raft messages to another node. A production transport
// can implement this with gRPC; tests can use an in-memory message bus.
type Transport interface {
	Send(context.Context, raftpb.Message) error
}

// Applied is emitted after a committed command has been decoded and applied.
type Applied struct {
	RangeID uint64
	Index   uint64
	Term    uint64
	Command Command
}

// Group owns exactly one Raft state machine and therefore one independent log
// for a single key range. The manager creates one Group per range descriptor.
type Group struct {
	descriptor RangeDescriptor
	node       raft.Node
	storage    raft.Storage
	transport  Transport
	applied    chan Applied

	mu     sync.RWMutex
	closed bool
}

type GroupConfig struct {
	Descriptor RangeDescriptor
	NodeID     uint64
	Peers      []raft.Peer
	Storage    raft.Storage
	Transport  Transport
	Applied    chan Applied
}

func NewGroup(config GroupConfig) (*Group, error) {
	if config.Descriptor.ID == 0 {
		return nil, fmt.Errorf("range id must be non-zero")
	}
	if config.NodeID == 0 {
		return nil, fmt.Errorf("node id must be non-zero")
	}
	if config.Descriptor.EndKey != "" && config.Descriptor.StartKey >= config.Descriptor.EndKey {
		return nil, fmt.Errorf("invalid range %q-%q", config.Descriptor.StartKey, config.Descriptor.EndKey)
	}
	if config.Storage == nil {
		config.Storage = raft.NewMemoryStorage()
	}
	if config.Applied == nil {
		config.Applied = make(chan Applied, 128)
	}

	raftConfig := &raft.Config{
		ID:                        config.NodeID,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   config.Storage,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 20,
		CheckQuorum:               true,
		PreVote:                   true,
	}

	var node raft.Node
	if len(config.Peers) == 0 {
		node = raft.RestartNode(raftConfig)
	} else {
		node = raft.StartNode(raftConfig, config.Peers)
	}

	return &Group{
		descriptor: config.Descriptor,
		node:       node,
		storage:    config.Storage,
		transport:  config.Transport,
		applied:    config.Applied,
	}, nil
}

func (g *Group) Descriptor() RangeDescriptor { return g.descriptor }

func (g *Group) Applied() <-chan Applied { return g.applied }

func (g *Group) Propose(ctx context.Context, command Command) error {
	if command.Key == "" {
		return fmt.Errorf("command key must not be empty")
	}
	if !g.descriptor.Contains(command.Key) {
		return fmt.Errorf("key %q is outside range %d", command.Key, g.descriptor.ID)
	}
	data, err := marshalCommand(command)
	if err != nil {
		return err
	}
	return g.node.Propose(ctx, data)
}

// Run drains Ready batches. It is deliberately owned by the caller so a
// server can run each range group under its own lifecycle and backpressure.
func (g *Group) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			g.Stop()
			return ctx.Err()
		case ready, ok := <-g.node.Ready():
			if !ok {
				return nil
			}
			if err := g.handleReady(ctx, ready); err != nil {
				return err
			}
		}
	}
}

func (g *Group) handleReady(ctx context.Context, ready raft.Ready) error {
	// HardState is part of the durable Raft state. raft.Storage exposes the
	// log-reading API, while concrete stores such as MemoryStorage and the
	// Pebble-backed store can also persist HardState through this hook.
	if !raft.IsEmptyHardState(ready.HardState) {
		if persistent, ok := g.storage.(interface {
			SetHardState(raftpb.HardState) error
		}); ok {
			if err := persistent.SetHardState(ready.HardState); err != nil {
				return fmt.Errorf("persist range %d hard state: %w", g.descriptor.ID, err)
			}
		}
	}
	if !raft.IsEmptyHardState(ready.HardState) || len(ready.Entries) > 0 {
		if err := g.storage.Append(ready.Entries); err != nil {
			return fmt.Errorf("append range %d raft log: %w", g.descriptor.ID, err)
		}
	}
	if g.transport != nil {
		for _, message := range ready.Messages {
			if err := g.transport.Send(ctx, message); err != nil {
				return fmt.Errorf("send range %d raft message: %w", g.descriptor.ID, err)
			}
		}
	}
	for _, entry := range ready.CommittedEntries {
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		command, err := unmarshalCommand(entry.Data)
		if err != nil {
			return fmt.Errorf("decode committed command at index %d: %w", entry.Index, err)
		}
		select {
		case g.applied <- Applied{RangeID: g.descriptor.ID, Index: entry.Index, Term: entry.Term, Command: command}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	g.node.Advance()
	return nil
}

func (g *Group) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	g.node.Stop()
}
