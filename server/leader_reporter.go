package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// leaderReporter keeps the metadata service's leader record current: when
// this node leads a range that metadata attributes to someone else, it
// proposes set_leader. Without this the router keeps sending requests to a
// crashed leader, which can never answer not_leader.
type leaderReporter struct {
	n        *Node
	baseURL  string
	interval time.Duration
	hc       *http.Client
}

type metadataRanges struct {
	Version int64 `json:"version"`
	Ranges  []struct {
		ID       uint64 `json:"id"`
		LeaderID uint64 `json:"leaderId"`
	} `json:"ranges"`
}

func (r *leaderReporter) run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.reconcile(ctx); err != nil && ctx.Err() == nil {
				log.Printf("leader reporter: %v", err)
			}
		}
	}
}

func (r *leaderReporter) reconcile(ctx context.Context) error {
	var leading []uint64
	for _, g := range r.n.manager.Groups() {
		if g.IsLeader() {
			leading = append(leading, g.Descriptor().ID)
		}
	}
	if len(leading) == 0 {
		return nil
	}
	snap, err := r.ranges(ctx)
	if err != nil {
		return err
	}
	for _, rid := range leading {
		for _, mr := range snap.Ranges {
			if mr.ID != rid || mr.LeaderID == r.n.cfg.NodeID {
				continue
			}
			body, _ := json.Marshal(map[string]any{
				"type": "set_leader", "expectedVersion": snap.Version, "rangeId": rid, "leaderId": r.n.cfg.NodeID,
			})
			status, err := r.post(ctx, "/api/metadata/commands", body)
			if err != nil {
				return err
			}
			if status == http.StatusConflict {
				return nil // version moved; retry on the next tick with fresh state
			}
			if status >= 300 {
				return fmt.Errorf("set_leader range %d: http %d", rid, status)
			}
			log.Printf("leader reporter: metadata now lists node %d as leader of range %d", r.n.cfg.NodeID, rid)
			return nil // one change per tick: each bumps the version
		}
	}
	return nil
}

func (r *leaderReporter) ranges(ctx context.Context) (*metadataRanges, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/api/metadata/ranges", nil)
	resp, err := r.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET ranges: http %d", resp.StatusCode)
	}
	var out metadataRanges
	return &out, json.NewDecoder(resp.Body).Decode(&out)
}

func (r *leaderReporter) post(ctx context.Context, path string, body []byte) (int, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.hc.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func newLeaderReporter(n *Node, baseURL string) *leaderReporter {
	return &leaderReporter{n: n, baseURL: strings.TrimRight(baseURL, "/"), interval: 300 * time.Millisecond, hc: &http.Client{Timeout: 2 * time.Second}}
}
