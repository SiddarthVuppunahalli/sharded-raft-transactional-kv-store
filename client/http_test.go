package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRouter mimics the Part 2 router's /api/kv contract.
func fakeRouter(t *testing.T, failFirst int32) (*httptest.Server, *atomic.Int32) {
	var mu sync.Mutex
	data := map[string]string{}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/kv" {
			http.NotFound(w, r)
			return
		}
		if calls.Add(1) <= failFirst {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error": "range leader 2 is unavailable"}`)
			return
		}
		key := r.URL.Query().Get("key")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			data[key] = string(b)
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			v, ok := data[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `{"error": "key does not exist"}`)
				return
			}
			_, _ = io.WriteString(w, v)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestHTTPClientPutGetAndMissingKey(t *testing.T) {
	srv, _ := fakeRouter(t, 0)
	c := NewHTTP(srv.URL + "/")
	ctx := context.Background()
	if err := c.Put(ctx, []byte("a key & more+"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	v, found, err := c.Get(ctx, []byte("a key & more+"))
	if err != nil || !found || string(v) != "v1" {
		t.Fatalf("get = %q %v %v", v, found, err)
	}
	_, found, err = c.Get(ctx, []byte("missing"))
	if err != nil || found {
		t.Fatalf("missing key: found=%v err=%v", found, err)
	}
}

func TestHTTPClientRetriesUnavailable(t *testing.T) {
	srv, calls := fakeRouter(t, 3)
	c := NewHTTP(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put after transient 503s: %v", err)
	}
	if calls.Load() != 4 {
		t.Fatalf("expected 3 retries then success, got %d calls", calls.Load())
	}
}

func TestHTTPClientGivesUpAtDeadline(t *testing.T) {
	srv, _ := fakeRouter(t, 1<<30)
	c := NewHTTP(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := c.Put(ctx, []byte("k"), []byte("v"))
	h, ok := IsHTTPError(err)
	if !ok || h.Status != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 error, got %v", err)
	}
}
