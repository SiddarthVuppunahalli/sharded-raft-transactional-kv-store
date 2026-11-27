// Package client is a range-aware Go client for the KV store. It learns the
// range map and current leaders from the Admin.Status RPC, sends each request
// to the leader of the key's range, and fails over to other nodes when a node
// is unreachable or has no leader.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb"
)

type Client struct {
	addrs []string

	mu       sync.RWMutex
	conns    map[string]*grpc.ClientConn
	ranges   []*kvpb.RangeStatus
	peers    map[uint64]string // node id -> addr as reachable by this client
	idByAddr map[string]uint64
	next     int

	// AttemptTimeout bounds a single RPC attempt against one node.
	AttemptTimeout time.Duration
}

// New creates a client. addrs are the node addresses reachable from the
// client, ordered by node id (addrs[0] is node 1, ...).
func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("at least one address is required")
	}
	c := &Client{addrs: addrs, conns: map[string]*grpc.ClientConn{}, peers: map[uint64]string{}, idByAddr: map[string]uint64{}, AttemptTimeout: time.Second}
	for i, a := range addrs {
		c.peers[uint64(i+1)] = a
		c.idByAddr[a] = uint64(i + 1)
	}
	return c, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, ok := c.conns[addr]
	c.mu.RUnlock()
	if ok {
		return conn, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// Status fetches status from a specific node address.
func (c *Client) Status(ctx context.Context, addr string) (*kvpb.StatusResponse, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return kvpb.NewAdminClient(conn).Status(ctx, &kvpb.StatusRequest{})
}

// Refresh reloads the range map / leaders from the first node that answers.
func (c *Client) Refresh(ctx context.Context) error {
	var lastErr error
	for _, a := range c.addrs {
		st, err := c.Status(ctx, a)
		if err != nil {
			lastErr = err
			continue
		}
		c.mu.Lock()
		c.ranges = st.Ranges
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("no node reachable: %w", lastErr)
}

// Leaders returns range id -> leader node id, asking every reachable node and
// trusting the node that reports itself as leader.
func (c *Client) Leaders(ctx context.Context) map[uint64]uint64 {
	out := map[uint64]uint64{}
	for _, a := range c.addrs {
		st, err := c.Status(ctx, a)
		if err != nil {
			continue
		}
		for _, r := range st.Ranges {
			if r.RaftState == "StateLeader" {
				out[r.Range.RangeId] = st.NodeId
			}
		}
	}
	return out
}

// targets returns node addresses to try for key, best guess (leader) first.
func (c *Client) targets(key []byte) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.addrs))
	for _, r := range c.ranges {
		d := r.Range
		if bytes.Compare(key, d.StartKey) >= 0 && (len(d.EndKey) == 0 || bytes.Compare(key, d.EndKey) < 0) {
			if a, ok := c.peers[r.LeaderNodeId]; ok {
				out = append(out, a)
			}
			break
		}
	}
	// Then the rest, round-robin so load spreads when the leader is unknown.
	c.next++
	for i := range c.addrs {
		a := c.addrs[(c.next+i)%len(c.addrs)]
		if len(out) == 0 || out[0] != a {
			out = append(out, a)
		}
	}
	return out
}

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

// do runs fn against candidate nodes until it succeeds, a non-retryable error
// occurs, or ctx expires. The range map is refreshed after a failure.
func (c *Client) do(ctx context.Context, key []byte, fn func(context.Context, *grpc.ClientConn) error) error {
	var lastErr error
	for attempt := 0; ; attempt++ {
		for _, addr := range c.targets(key) {
			if ctx.Err() != nil {
				if lastErr == nil {
					lastErr = ctx.Err()
				}
				return lastErr
			}
			conn, err := c.conn(addr)
			if err != nil {
				lastErr = err
				continue
			}
			// Bound each attempt so one unresponsive node (e.g. a partitioned
			// former leader) cannot consume the whole operation budget.
			actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
			err = fn(actx, conn)
			cancel()
			if err == nil {
				return nil
			}
			lastErr = err
			if !retryable(err) {
				return err
			}
		}
		_ = c.Refresh(ctx)
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(time.Duration(20*(attempt+1)) * time.Millisecond):
		}
	}
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.do(ctx, key, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := kvpb.NewKVClient(conn).Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		return err
	})
}

func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	err = c.do(ctx, key, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := kvpb.NewKVClient(conn).Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		value, found = resp.Value, resp.Found
		return nil
	})
	return
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.do(ctx, key, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := kvpb.NewKVClient(conn).Delete(ctx, &kvpb.DeleteRequest{Key: key})
		return err
	})
}

// Txn sends a transaction to any node (the coordinator routes internally).
func (c *Client) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	var resp *kvpb.TxnResponse
	var key []byte
	if len(req.Ops) > 0 {
		key = req.Ops[0].Key
	}
	err := c.do(ctx, key, func(ctx context.Context, conn *grpc.ClientConn) error {
		r, err := kvpb.NewKVClient(conn).Txn(ctx, req)
		resp = r
		return err
	})
	return resp, err
}
