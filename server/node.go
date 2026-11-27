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
	"net/http"
	"os"
	"path/filepath"
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
	HTTPListenAddr string                    // "" = no HTTP range API
	HTTPPeers      map[uint64]string         // node id -> advertised HTTP host:port
	MetadataURL    string                    // metadata service base URL; "" = do not report leaders
	DataDir        string                    // "" = in-memory; otherwise Pebble per range under this dir
	StateMachine   StateMachine              // nil = in-memory map (ignored when DataDir is set)
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

	storages []*multiraft.PebbleStorage
	groupsWG sync.WaitGroup

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
	if cfg.StateMachine == nil && cfg.DataDir == "" {
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
	pebbleSM := &pebbleStateMachine{byRange: map[uint64]*multiraft.PebbleStorage{}}
	if cfg.DataDir != "" {
		n.sm = pebbleSM
	}
	for _, desc := range cfg.Ranges {
		gc := multiraft.GroupConfig{
			Descriptor: desc,
			NodeID:     cfg.NodeID,
			Peers:      raftPeers,
			Transport:  tr.ForRange(desc.ID),
			Applied:    n.applied,
		}
		if cfg.DataDir != "" {
			dir := filepath.Join(cfg.DataDir, fmt.Sprintf("range-%d", desc.ID))
			if err := os.MkdirAll(dir, 0o755); err != nil {
				n.closeStorages()
				return nil, err
			}
			st, err := multiraft.OpenPebbleStorage(dir)
			if err != nil {
				n.closeStorages()
				return nil, err
			}
			n.storages = append(n.storages, st)
			pebbleSM.byRange[desc.ID] = st
			gc.Storage = st
			restart, err := st.HasState()
			if err != nil {
				n.closeStorages()
				return nil, err
			}
			if restart {
				// Recover log, hard state and membership from Pebble.
				gc.Peers = nil
				log.Printf("range %d: recovering from %s", desc.ID, dir)
			}
		}
		g, err := multiraft.NewGroup(gc)
		if err != nil {
			n.closeStorages()
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
		n.groupsWG.Add(1)
		go func() {
			defer n.groupsWG.Done()
			if err := g.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("range %d raft loop stopped: %v", g.Descriptor().ID, err)
			}
		}()
	}

	go func() {
		<-ctx.Done()
		n.grpc.Stop()
		n.transport.Close()
		n.groupsWG.Wait()
		n.closeStorages()
	}()
	if n.cfg.HTTPListenAddr != "" {
		hs := &http.Server{Addr: n.cfg.HTTPListenAddr, Handler: n.HTTPHandler(), ReadHeaderTimeout: 5 * time.Second}
		hl, err := net.Listen("tcp", n.cfg.HTTPListenAddr)
		if err != nil {
			return err
		}
		go func() {
			if err := hs.Serve(hl); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("http range API stopped: %v", err)
			}
		}()
		go func() {
			<-ctx.Done()
			_ = hs.Close()
		}()
		log.Printf("node %d HTTP range API on %s", n.cfg.NodeID, n.cfg.HTTPListenAddr)
	}
	if n.cfg.MetadataURL != "" {
		go newLeaderReporter(n, n.cfg.MetadataURL).run(ctx)
		log.Printf("node %d reporting range leadership to %s", n.cfg.NodeID, n.cfg.MetadataURL)
	}
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
	if g.LeaderID() == raft.None {
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

func (n *Node) closeStorages() {
	if p, ok := n.sm.(*pebbleStateMachine); ok {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
	}
	for _, st := range n.storages {
		_ = st.Close()
	}
	n.storages = nil
}

func (n *Node) appliedIndexOf(rangeID uint64) uint64 {
	n.amu.RLock()
	defer n.amu.RUnlock()
	return n.appliedIndex[rangeID]
}

// pebbleStateMachine serves results from Pebble. Writes are already
// persisted by the Raft group (PebbleStorage.Apply) before the Applied event
// is emitted, so only reads need work here.
type pebbleStateMachine struct {
	mu      sync.RWMutex
	closed  bool
	byRange map[uint64]*multiraft.PebbleStorage
}

func (p *pebbleStateMachine) Apply(rangeID, _ uint64, cmd multiraft.Command) ([]byte, bool) {
	if cmd.Op != "get" {
		return nil, true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	st, ok := p.byRange[rangeID]
	if !ok || p.closed {
		return nil, false
	}
	v, found, err := st.Get(cmd.Key)
	if err != nil {
		log.Printf("range %d: read %q: %v", rangeID, cmd.Key, err)
		return nil, false
	}
	return v, found
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

// RangeStats counts keys (and their bytes) that fall inside desc.
func (m *MemoryStateMachine) RangeStats(desc multiraft.RangeDescriptor) (int, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys, bytes := 0, int64(0)
	for k, v := range m.data {
		if desc.Contains(k) {
			keys++
			bytes += int64(len(k) + len(v))
		}
	}
	return keys, bytes
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
