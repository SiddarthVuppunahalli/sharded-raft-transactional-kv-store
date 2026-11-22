package server

import (
	"context"
	"errors"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb"
	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/multiraft"
)

type kvService struct {
	kvpb.UnimplementedKVServer
	n *Node
}

// toStatus maps internal failures to gRPC codes. UNAVAILABLE and
// DEADLINE_EXCEEDED are retryable by clients; a DEADLINE_EXCEEDED write has
// an unknown outcome (it may still commit).
func toStatus(err error) error {
	switch {
	case errors.Is(err, errNoLeader), errors.Is(err, raft.ErrStopped):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request timed out waiting for raft commit")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *kvService) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, g, err := s.n.propose(ctx, multiraft.Command{Op: "get", Key: string(req.Key)})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.GetResponse{Found: res.found, Value: res.value, RangeId: g.Descriptor().ID, RaftIndex: res.index}, nil
}

func (s *kvService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, g, err := s.n.propose(ctx, multiraft.Command{Op: "put", Key: string(req.Key), Value: req.Value})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.PutResponse{RangeId: g.Descriptor().ID, RaftIndex: res.index}, nil
}

func (s *kvService) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, g, err := s.n.propose(ctx, multiraft.Command{Op: "delete", Key: string(req.Key)})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.DeleteResponse{RangeId: g.Descriptor().ID, RaftIndex: res.index}, nil
}

func (s *kvService) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	if s.n.cfg.Txn == nil {
		return nil, status.Error(codes.Unimplemented, "transaction layer not registered on this node")
	}
	return s.n.cfg.Txn.Txn(ctx, req)
}

type adminService struct {
	kvpb.UnimplementedAdminServer
	n *Node
}

func (s *adminService) Status(context.Context, *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	resp := &kvpb.StatusResponse{NodeId: s.n.cfg.NodeID, Addr: s.n.cfg.Peers[s.n.cfg.NodeID], Peers: s.n.cfg.Peers}
	for _, g := range s.n.manager.Groups() {
		st := g.Status()
		d := g.Descriptor()
		ids := make([]uint64, 0, len(s.n.cfg.Peers))
		for id := range s.n.cfg.Peers {
			ids = append(ids, id)
		}
		resp.Ranges = append(resp.Ranges, &kvpb.RangeStatus{
			Range:        &kvpb.RangeDescriptor{RangeId: d.ID, StartKey: []byte(d.StartKey), EndKey: []byte(d.EndKey), ReplicaNodeIds: ids},
			LeaderNodeId: st.Lead,
			Term:         st.Term,
			CommitIndex:  st.Commit,
			AppliedIndex: s.n.appliedIndexOf(d.ID),
			RaftState:    st.RaftState.String(),
		})
	}
	return resp, nil
}
