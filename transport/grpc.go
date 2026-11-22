// Package transport implements the inter-node Raft transport over gRPC.
//
// One GRPCTransport exists per node. It multiplexes the Raft messages of
// every range group hosted on the node over a single client connection per
// peer. Sends are asynchronous and never fail the caller: a full queue or an
// unreachable peer just drops messages (Raft retransmits) and reports the
// peer as unreachable to the owning range group.
package transport

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/multiraft"
)

const (
	queueSize    = 4096
	maxBatch     = 256
	sendTimeout  = 1 * time.Second
	logEveryFail = 50
)

// GroupLookup resolves a range ID to its local Raft group. *multiraft.Manager
// satisfies it.
type GroupLookup interface {
	Group(rangeID uint64) (*multiraft.Group, bool)
}

type GRPCTransport struct {
	nodeID uint64
	groups GroupLookup
	peers  map[uint64]*peer // immutable after New
}

type peer struct {
	id     uint64
	addr   string
	queue  chan *kvpb.RaftMessage
	conn   *grpc.ClientConn
	client kvpb.RaftTransportClient
	fails  int
}

func New(nodeID uint64, peerAddrs map[uint64]string, groups GroupLookup) (*GRPCTransport, error) {
	t := &GRPCTransport{nodeID: nodeID, groups: groups, peers: make(map[uint64]*peer)}
	for id, addr := range peerAddrs {
		if id == nodeID {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial peer %d (%s): %w", id, addr, err)
		}
		p := &peer{id: id, addr: addr, queue: make(chan *kvpb.RaftMessage, queueSize), conn: conn, client: kvpb.NewRaftTransportClient(conn)}
		t.peers[id] = p
	}
	return t, nil
}

// Start launches one sender goroutine per peer.
func (t *GRPCTransport) Start(ctx context.Context) {
	for _, p := range t.peers {
		go t.sendLoop(ctx, p)
	}
}

func (t *GRPCTransport) Close() {
	for _, p := range t.peers {
		_ = p.conn.Close()
	}
}

// ForRange returns a multiraft.Transport bound to one range group.
func (t *GRPCTransport) ForRange(rangeID uint64) multiraft.Transport {
	return rangeTransport{t: t, rangeID: rangeID}
}

type rangeTransport struct {
	t       *GRPCTransport
	rangeID uint64
}

func (r rangeTransport) Send(_ context.Context, m raftpb.Message) error {
	r.t.enqueue(r.rangeID, m)
	return nil // never block or fail the Raft Ready loop
}

func (t *GRPCTransport) enqueue(rangeID uint64, m raftpb.Message) {
	p, ok := t.peers[m.To]
	if !ok {
		return
	}
	data, err := m.Marshal()
	if err != nil {
		log.Printf("transport: marshal raft message: %v", err)
		return
	}
	msg := &kvpb.RaftMessage{RangeId: rangeID, FromNodeId: m.From, ToNodeId: m.To, Message: data}
	select {
	case p.queue <- msg:
	default:
		t.reportUnreachable(rangeID, p.id) // queue full: peer is slow or gone
	}
}

func (t *GRPCTransport) sendLoop(ctx context.Context, p *peer) {
	batch := make([]*kvpb.RaftMessage, 0, maxBatch)
	for {
		batch = batch[:0]
		select {
		case <-ctx.Done():
			return
		case m := <-p.queue:
			batch = append(batch, m)
		}
	drain:
		for len(batch) < maxBatch {
			select {
			case m := <-p.queue:
				batch = append(batch, m)
			default:
				break drain
			}
		}
		sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
		_, err := p.client.Send(sendCtx, &kvpb.RaftMessageBatch{Messages: batch})
		cancel()
		if err != nil {
			p.fails++
			if p.fails%logEveryFail == 1 {
				log.Printf("transport: send to node %d (%s) failed (%d consecutive): %v", p.id, p.addr, p.fails, err)
			}
			seen := map[uint64]bool{}
			for _, m := range batch {
				if !seen[m.RangeId] {
					seen[m.RangeId] = true
					t.reportUnreachable(m.RangeId, p.id)
				}
			}
			continue
		}
		if p.fails > 0 {
			log.Printf("transport: node %d (%s) reachable again", p.id, p.addr)
			p.fails = 0
		}
	}
}

func (t *GRPCTransport) reportUnreachable(rangeID, nodeID uint64) {
	if g, ok := t.groups.Group(rangeID); ok {
		g.ReportUnreachable(nodeID)
	}
}

// Server is the receiving side: the RaftTransport gRPC service.
type Server struct {
	kvpb.UnimplementedRaftTransportServer
	Groups GroupLookup
}

func (s *Server) Send(ctx context.Context, batch *kvpb.RaftMessageBatch) (*kvpb.RaftMessageAck, error) {
	var accepted uint32
	for _, m := range batch.Messages {
		g, ok := s.Groups.Group(m.RangeId)
		if !ok {
			continue
		}
		var msg raftpb.Message
		if err := msg.Unmarshal(m.Message); err != nil {
			continue
		}
		if err := g.Step(ctx, msg); err == nil {
			accepted++
		}
	}
	return &kvpb.RaftMessageAck{Accepted: accepted}, nil
}
