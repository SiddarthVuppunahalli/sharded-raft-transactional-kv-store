package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/client"
)

func call(t *testing.T, n *Node, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	rec := httptest.NewRecorder()
	n.HTTPHandler().ServeHTTP(rec, httptest.NewRequest(method, path, &buf))
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func execute(op string, rangeID int, key, value string) map[string]any {
	return map[string]any{"operation": op, "rangeId": rangeID, "key": key, "value": value, "metadataVersion": 3}
}

// TestHTTPRangeAPIContract checks the responses the metadata router relies
// on: found=true on writes, not_leader with leaderId, and stale_metadata.
func TestHTTPRangeAPIContract(t *testing.T) {
	tc := startCluster(t, 3)
	c, err := client.New(tc.addrs)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	leaders := waitLeaders(t, c, 2)
	leader := tc.nodes[leaders[1]]
	var follower *Node
	for id, n := range tc.nodes {
		if id != leaders[1] {
			follower = n
			break
		}
	}

	code, put := call(t, leader, http.MethodPost, "/_range/execute", execute("put", 1, "apple", "red"))
	if code != 200 || put["found"] != true || put["committed"] != true || put["value"] != "red" {
		t.Fatalf("put: %d %v", code, put)
	}
	if put["leaderId"] != float64(leaders[1]) || put["leaderNodeId"] != float64(leaders[1]) || put["metadataVersion"] != float64(3) {
		t.Fatalf("put leader/version fields: %v", put)
	}

	code, get := call(t, leader, http.MethodPost, "/_range/execute", execute("get", 1, "apple", ""))
	if code != 200 || get["found"] != true || get["value"] != "red" {
		t.Fatalf("get: %d %v", code, get)
	}
	code, miss := call(t, leader, http.MethodPost, "/_range/execute", execute("get", 1, "avocado", ""))
	if code != 200 || miss["found"] != false || miss["value"] != nil {
		t.Fatalf("get missing: %d %v", code, miss)
	}

	code, nl := call(t, follower, http.MethodPost, "/_range/execute", execute("put", 1, "apple", "green"))
	if code != http.StatusConflict || nl["error"] != "not_leader" || nl["leaderId"] != float64(leaders[1]) {
		t.Fatalf("follower: %d %v", code, nl)
	}

	code, stale := call(t, leader, http.MethodPost, "/_range/execute", execute("put", 1, "zebra", "x"))
	if code != http.StatusConflict || stale["error"] != "stale_metadata" {
		t.Fatalf("key outside range: %d %v", code, stale)
	}
	code, unknown := call(t, leader, http.MethodPost, "/_range/execute", execute("get", 99, "apple", ""))
	if code != http.StatusConflict || unknown["error"] != "stale_metadata" {
		t.Fatalf("unknown range: %d %v", code, unknown)
	}
	code, bad := call(t, leader, http.MethodPost, "/_range/execute", execute("scan", 1, "apple", ""))
	if code != http.StatusBadRequest {
		t.Fatalf("bad op: %d %v", code, bad)
	}

	code, stats := call(t, leader, http.MethodGet, "/_range/stats?rangeId=1", nil)
	if code != 200 || stats["keyCount"] != float64(1) || stats["leaderNodeId"] != float64(leaders[1]) || stats["replicaCount"] != float64(3) {
		t.Fatalf("stats: %d %v", code, stats)
	}

	code, meta := call(t, leader, http.MethodGet, "/_range/metadata?rangeId=1", nil)
	reps, _ := meta["replicas"].([]any)
	if code != 200 || meta["startKey"] != nil || meta["endKey"] != "m" || len(reps) != 3 {
		t.Fatalf("metadata: %d %v", code, meta)
	}
	leaders2 := 0
	for _, r := range reps {
		if r.(map[string]any)["role"] == "leader" {
			leaders2++
		}
	}
	if leaders2 != 1 {
		t.Fatalf("metadata should list exactly one leader: %v", reps)
	}
}
