package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/multiraft"
)

// HTTP range API used by the metadata router (Part 2). Contract agreed in
// the team chat:
//
//	POST /_range/execute   {"operation","rangeId","key","value","metadataVersion"}
//	GET  /_range/stats?rangeId=N
//	GET  /_range/metadata?rangeId=N
//
// Routing errors are returned as {"error": "..."} so the router can react:
//
//	not_leader      (409) this node is not the range leader; leaderId says who is
//	stale_metadata  (409) unknown range or key outside the range; refresh the map
//	no_leader       (503) the range has no elected leader / no quorum
//	timeout         (504) the write may or may not have committed
//
// Responses carry the leader as both "leaderId" and "leaderNodeId".

type executeRequest struct {
	Operation       string `json:"operation"`
	RangeID         uint64 `json:"rangeId"`
	Key             string `json:"key"`
	Value           string `json:"value"`
	MetadataVersion int64  `json:"metadataVersion"`
}

type executeResponse struct {
	RangeID         uint64  `json:"rangeId"`
	Operation       string  `json:"operation"`
	Key             string  `json:"key"`
	Value           *string `json:"value"`
	Found           bool    `json:"found"`
	Committed       bool    `json:"committed"`
	LeaderID        uint64  `json:"leaderId"`
	LeaderNodeID    uint64  `json:"leaderNodeId"`
	MetadataVersion int64   `json:"metadataVersion"`
	RaftIndex       uint64  `json:"raftIndex"`
}

type errorResponse struct {
	Error        string `json:"error"`
	Detail       string `json:"detail,omitempty"`
	LeaderID     uint64 `json:"leaderId,omitempty"`
	LeaderNodeID uint64 `json:"leaderNodeId,omitempty"`
	RangeID      uint64 `json:"rangeId,omitempty"`
}

type rangeStats struct {
	RangeID      uint64 `json:"rangeId"`
	KeyCount     int    `json:"keyCount"`
	SizeBytes    int64  `json:"sizeBytes"`
	LogEntries   uint64 `json:"logEntries"`
	CommitIndex  uint64 `json:"commitIndex"`
	AppliedIndex uint64 `json:"appliedIndex"`
	LeaderNodeID uint64 `json:"leaderNodeId"`
	ReplicaCount int    `json:"replicaCount"`
}

type replicaInfo struct {
	NodeID  uint64 `json:"nodeId"`
	APIURL  string `json:"apiUrl"`
	Role    string `json:"role"`
	Healthy bool   `json:"healthy"`
}

type rangeMetadata struct {
	RangeID         uint64        `json:"rangeId"`
	StartKey        *string       `json:"startKey"` // null = unbounded
	EndKey          *string       `json:"endKey"`   // null = unbounded
	LeaderNodeID    uint64        `json:"leaderNodeId"`
	MetadataVersion int64         `json:"metadataVersion"`
	Replicas        []replicaInfo `json:"replicas"`
}

// HTTPHandler returns the HTTP range API for this node.
func (n *Node) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_range/execute", n.handleExecute)
	mux.HandleFunc("/_range/stats", n.handleStats)
	mux.HandleFunc("/_range/metadata", n.handleMetadata)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// leaderCheck returns the group if this node can serve as its leader, or
// writes the routing error and returns nil.
func (n *Node) leaderCheck(w http.ResponseWriter, rangeID uint64) *multiraft.Group {
	g, ok := n.manager.Group(rangeID)
	if !ok {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "stale_metadata", Detail: "unknown range", RangeID: rangeID})
		return nil
	}
	lead := g.LeaderID()
	switch {
	case lead == 0:
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "no_leader", Detail: "range has no elected leader", RangeID: rangeID})
		return nil
	case lead != n.cfg.NodeID:
		writeJSON(w, http.StatusConflict, errorResponse{Error: "not_leader", LeaderID: lead, LeaderNodeID: lead, RangeID: rangeID})
		return nil
	}
	return g
}

func (n *Node) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method_not_allowed"})
		return
	}
	var req executeRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "bad_request", Detail: err.Error()})
		return
	}
	if req.Key == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "bad_request", Detail: "key is required"})
		return
	}
	var op string
	switch req.Operation {
	case "get", "put", "delete":
		op = req.Operation
	default:
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "bad_request", Detail: "operation must be get, put or delete"})
		return
	}
	g := n.leaderCheck(w, req.RangeID)
	if g == nil {
		return
	}
	if !g.Descriptor().Contains(req.Key) {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "stale_metadata", Detail: "key is outside this range", RangeID: req.RangeID})
		return
	}
	cmd := multiraft.Command{Op: op, Key: req.Key}
	if op == "put" {
		cmd.Value = []byte(req.Value)
	}
	res, _, err := n.propose(r.Context(), cmd)
	if err != nil {
		switch {
		case errors.Is(err, errNoLeader):
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "no_leader", Detail: err.Error(), RangeID: req.RangeID})
		case errors.Is(err, context.DeadlineExceeded):
			writeJSON(w, http.StatusGatewayTimeout, errorResponse{Error: "timeout", Detail: "raft commit timed out; outcome unknown", RangeID: req.RangeID})
		default:
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "unavailable", Detail: err.Error(), RangeID: req.RangeID})
		}
		return
	}
	lead := g.LeaderID()
	out := executeResponse{
		RangeID: req.RangeID, Operation: op, Key: req.Key, Committed: true,
		LeaderID: lead, LeaderNodeID: lead, MetadataVersion: req.MetadataVersion, RaftIndex: res.index,
	}
	switch op {
	case "get":
		out.Found = res.found
		if res.found {
			v := string(res.value)
			out.Value = &v
		}
	case "put":
		// The router requires found=true to confirm a write.
		out.Found = true
		v := req.Value
		out.Value = &v
	case "delete":
		out.Found = res.found
	}
	writeJSON(w, http.StatusOK, out)
}

func rangeIDParam(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	id, err := strconv.ParseUint(r.URL.Query().Get("rangeId"), 10, 64)
	if err != nil || id == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "bad_request", Detail: "rangeId query parameter is required"})
		return 0, false
	}
	return id, true
}

func (n *Node) handleStats(w http.ResponseWriter, r *http.Request) {
	id, ok := rangeIDParam(w, r)
	if !ok {
		return
	}
	g, found := n.manager.Group(id)
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "stale_metadata", Detail: "unknown range", RangeID: id})
		return
	}
	st := g.Status()
	out := rangeStats{
		RangeID: id, LeaderNodeID: st.Lead, CommitIndex: st.Commit,
		AppliedIndex: n.appliedIndexOf(id), ReplicaCount: len(n.cfg.Peers),
	}
	switch sm := n.sm.(type) {
	case *pebbleStateMachine:
		sm.mu.RLock()
		if ps, ok := sm.byRange[id]; ok && !sm.closed {
			out.KeyCount, out.SizeBytes, _ = ps.KVStats()
			out.LogEntries, _ = ps.LastIndex()
		}
		sm.mu.RUnlock()
	case *MemoryStateMachine:
		out.KeyCount, out.SizeBytes = sm.RangeStats(g.Descriptor())
		out.LogEntries = st.Commit
	}
	writeJSON(w, http.StatusOK, out)
}

func (n *Node) handleMetadata(w http.ResponseWriter, r *http.Request) {
	id, ok := rangeIDParam(w, r)
	if !ok {
		return
	}
	g, found := n.manager.Group(id)
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "stale_metadata", Detail: "unknown range", RangeID: id})
		return
	}
	d := g.Descriptor()
	st := g.Status()
	out := rangeMetadata{RangeID: id, LeaderNodeID: st.Lead, MetadataVersion: 1}
	if d.StartKey != "" {
		s := d.StartKey
		out.StartKey = &s
	}
	if d.EndKey != "" {
		e := d.EndKey
		out.EndKey = &e
	}
	ids := make([]uint64, 0, len(n.cfg.Peers))
	for pid := range n.cfg.Peers {
		ids = append(ids, pid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, pid := range ids {
		role := "follower"
		if pid == st.Lead {
			role = "leader"
		}
		// Health is only known precisely on the leader (Raft progress); other
		// replicas report themselves and the leader they hear from as healthy.
		healthy := pid == n.cfg.NodeID || pid == st.Lead
		if pr, ok := st.Progress[pid]; ok && st.Lead == n.cfg.NodeID {
			healthy = pid == n.cfg.NodeID || pr.RecentActive
		}
		url := ""
		if a, ok := n.cfg.HTTPPeers[pid]; ok {
			url = "http://" + a
		}
		out.Replicas = append(out.Replicas, replicaInfo{NodeID: pid, APIURL: url, Role: role, Healthy: healthy})
	}
	writeJSON(w, http.StatusOK, out)
}
