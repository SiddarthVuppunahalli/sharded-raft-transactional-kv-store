// Package server wires a node together: it hosts one Raft group per key
// range, applies committed commands to a state machine, and exposes the KV,
// Admin, RaftTransport and (optionally) transaction gRPC services.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/multiraft"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/transport"
)

// StateMachine applies committed commands for one node. The default is an
// in-memory map; the storage layer (Pebble + MVCC) can replace it.
type StateMachine interface {
	// Apply executes a committed command and returns the value (for reads).
	Apply(rangeID, index uint64, cmd multiraft.Command) (value []byte, found bool)
}

// TxnHandler serves the client Txn RPC. Implemented by the 2PC coordinator.
type TxnHandler interface {
	Txn(context.Context, *kvpb.TxnRequest) (*kvpb.TxnResponse, error)
}

type Config struct {
	NodeID         uint64
	ListenAddr     string
	Peers          map[uint64]string // node id -> advertised address (includes self)
	Ranges         []multiraft.RangeDescriptor
	RequestTimeout time.Duration
	StateMachine   StateMachine              // nil = in-memory map
	Txn            TxnHandler                // nil = Txn returns UNIMPLEMENTED
	Participant    kvpb.TxnParticipantServer // nil = not registered
}

type Node struct {
	cfg       Config
	manager   *multiraft.Manager
	transport *transport.GRPCTransport
	sm        StateMachine
	applied   chan multiraft.Applied
	grpc      *grpc.Server

	reqSeq  atomic.Uint64
	pmu     sync.Mutex
	pending map[string]chan applyResult

	amu          sync.RWMutex
	appliedIndex map[uint64]uint64
}

type applyResult struct {
	value []byte
	found bool
	index uint64
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	if cfg.StateMachine == nil {
		cfg.StateMachine = NewMemoryStateMachine()
	}
	n := &Node{
		cfg:          cfg,
		manager:      multiraft.NewManager(),
		sm:           cfg.StateMachine,
		applied:      make(chan multiraft.Applied, 1024),
		pending:      make(map[string]chan applyResult),
		appliedIndex: make(map[uint64]uint64),
	}
	tr, err := transport.New(cfg.NodeID, cfg.Peers, n.manager)
	if err != nil {
		return nil, err
	}
	n.transport = tr

	peerIDs := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		peerIDs = append(peerIDs, id)
	}
	sort.Slice(peerIDs, func(i, j int) bool { return peerIDs[i] < peerIDs[j] })
	raftPeers := make([]raft.Peer, len(peerIDs))
	for i, id := range peerIDs {
		raftPeers[i] = raft.Peer{ID: id}
	}
	for _, desc := range cfg.Ranges {
		g, err := multiraft.NewGroup(multiraft.GroupConfig{
			Descriptor: desc,
			NodeID:     cfg.NodeID,
			Peers:      raftPeers,
			Transport:  tr.ForRange(desc.ID),
			Applied:    n.applied,
		})
		if err != nil {
			return nil, err
		}
		if err := n.manager.Add(g); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (n *Node) Manager() *multiraft.Manager { return n.manager }

// Serve starts Raft groups, the transport and the gRPC server; it blocks
// until ctx is cancelled or the listener fails.
func (n *Node) Serve(ctx context.Context) error {
	lis, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return err
	}
	n.grpc = grpc.NewServer()
	kvpb.RegisterKVServer(n.grpc, &kvService{n: n})
	kvpb.RegisterAdminServer(n.grpc, &adminService{n: n})
	kvpb.RegisterRaftTransportServer(n.grpc, &transport.Server{Groups: n.manager})
	if n.cfg.Participant != nil {
		kvpb.RegisterTxnParticipantServer(n.grpc, n.cfg.Participant)
	}

	n.transport.Start(ctx)
	go n.applyLoop(ctx)
	for _, g := range n.manager.Groups() {
		g := g
		go func() {
			if err := g.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("range %d raft loop stopped: %v", g.Descriptor().ID, err)
			}
		}()
	}

	go func() {
		<-ctx.Done()
		n.grpc.GracefulStop()
		n.transport.Close()
	}()
	log.Printf("node %d serving on %s with %d ranges, peers=%v", n.cfg.NodeID, n.cfg.ListenAddr, len(n.cfg.Ranges), n.cfg.Peers)
	return n.grpc.Serve(lis)
}

func (n *Node) applyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-n.applied:
			value, found := n.sm.Apply(a.RangeID, a.Index, a.Command)
			n.amu.Lock()
			n.appliedIndex[a.RangeID] = a.Index
			n.amu.Unlock()
			if a.Command.ReqID == "" {
				continue
			}
			n.pmu.Lock()
			ch, ok := n.pending[a.Command.ReqID]
			delete(n.pending, a.Command.ReqID)
			n.pmu.Unlock()
			if ok {
				ch <- applyResult{value: value, found: found, index: a.Index}
			}
		}
	}
}

var errNoLeader = errors.New("range has no leader / proposal dropped")

// propose replicates cmd through the owning range's Raft log and waits for
// it to be applied locally. Reads also go through the log, which makes them
// linearizable even when served by a follower.
func (n *Node) propose(ctx context.Context, cmd multiraft.Command) (applyResult, *multiraft.Group, error) {
	g, err := n.manager.Lookup(cmd.Key)
	if err != nil {
		return applyResult{}, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()

	cmd.ReqID = fmt.Sprintf("%d-%d", n.cfg.NodeID, n.reqSeq.Add(1))
	ch := make(chan applyResult, 1)
	n.pmu.Lock()
	n.pending[cmd.ReqID] = ch
	n.pmu.Unlock()
	defer func() {
		n.pmu.Lock()
		delete(n.pending, cmd.ReqID)
		n.pmu.Unlock()
	}()

	// etcd/raft blocks proposals (rather than failing them) while a replica
	// knows no leader, e.g. when it is partitioned away. Fail fast so the
	// client can retry on a replica that can reach the quorum.
	if g.Leader() == raft.None {
		return applyResult{}, g, errNoLeader
	}
	if err := g.Propose(ctx, cmd); err != nil {
		if errors.Is(err, raft.ErrProposalDropped) {
			return applyResult{}, g, errNoLeader
		}
		return applyResult{}, g, err
	}
	select {
	case res := <-ch:
		return res, g, nil
	case <-ctx.Done():
		return applyResult{}, g, ctx.Err()
	}
}

func (n *Node) appliedIndexOf(rangeID uint64) uint64 {
	n.amu.RLock()
	defer n.amu.RUnlock()
	return n.appliedIndex[rangeID]
}

// MemoryStateMachine is a simple per-range map, used until the Pebble/MVCC
// storage engine is plugged in via Config.StateMachine.
type MemoryStateMachine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStateMachine() *MemoryStateMachine {
	return &MemoryStateMachine{data: make(map[string][]byte)}
}

func (m *MemoryStateMachine) Apply(_ uint64, _ uint64, cmd multiraft.Command) ([]byte, bool) {
	switch cmd.Op {
	case "put":
		m.mu.Lock()
		m.data[cmd.Key] = append([]byte(nil), cmd.Value...)
		m.mu.Unlock()
		return nil, true
	case "delete":
		m.mu.Lock()
		_, ok := m.data[cmd.Key]
		delete(m.data, cmd.Key)
		m.mu.Unlock()
		return nil, ok
	case "get":
		m.mu.RLock()
		v, ok := m.data[cmd.Key]
		m.mu.RUnlock()
		return v, ok
	}
	return nil, false
}
