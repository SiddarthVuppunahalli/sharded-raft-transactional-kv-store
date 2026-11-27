package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPClient talks to the metadata router's public KV API (Part 2):
//
//	GET <base>/api/kv?key=K  -> 200 value | 404 missing
//	PUT <base>/api/kv?key=K  (body = value) -> 204
//
// The router resolves the range and forwards to the range leader. Errors are
// JSON {"error": "..."}; 503 and 504 are retried until ctx expires.
type HTTPClient struct {
	base string
	hc   *http.Client
	// AttemptTimeout bounds one HTTP request.
	AttemptTimeout time.Duration
}

// HTTPError is a non-2xx response from the router.
type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("http %d: %s", e.Status, e.Message) }

// Retryable reports whether the router signalled a transient failure
// (unavailable leader/quorum, timeout, or bad upstream response).
func (e *HTTPError) Retryable() bool {
	return e.Status == http.StatusServiceUnavailable || e.Status == http.StatusGatewayTimeout || e.Status == http.StatusBadGateway
}

func NewHTTP(baseURL string) *HTTPClient {
	return &HTTPClient{
		base:           strings.TrimRight(baseURL, "/"),
		hc:             &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}},
		AttemptTimeout: 6 * time.Second, // router waits up to 5s for the range leader
	}
}

func (c *HTTPClient) url(key []byte) string {
	return c.base + "/api/kv?key=" + url.QueryEscape(string(key))
}

func (c *HTTPClient) do(ctx context.Context, method string, key, body []byte) (int, []byte, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(actx, method, c.url(key), rd)
		if err != nil {
			cancel()
			return 0, nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "text/plain; charset=utf-8")
		}
		resp, err := c.hc.Do(req)
		if err == nil {
			data, rerr := io.ReadAll(resp.Body)
			resp.Body.Close()
			cancel()
			if rerr != nil {
				lastErr = rerr
			} else if resp.StatusCode < 400 || resp.StatusCode == http.StatusNotFound {
				return resp.StatusCode, data, nil
			} else {
				herr := &HTTPError{Status: resp.StatusCode, Message: strings.TrimSpace(string(data))}
				if !herr.Retryable() {
					return resp.StatusCode, nil, herr
				}
				lastErr = herr
			}
		} else {
			cancel()
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return 0, nil, lastErr
		case <-time.After(time.Duration(20*(attempt+1)) * time.Millisecond):
		}
	}
}

func (c *HTTPClient) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	status, data, err := c.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		// 404 is either "key does not exist" or "no range contains the key";
		// only the former is a successful read.
		if strings.Contains(string(data), "no range") {
			return nil, false, &HTTPError{Status: status, Message: strings.TrimSpace(string(data))}
		}
		return nil, false, nil
	}
	return data, true, nil
}

func (c *HTTPClient) Put(ctx context.Context, key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	status, data, err := c.do(ctx, http.MethodPut, key, value)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return &HTTPError{Status: status, Message: strings.TrimSpace(string(data))}
	}
	return nil
}

// IsHTTPError extracts an *HTTPError from err.
func IsHTTPError(err error) (*HTTPError, bool) {
	var h *HTTPError
	ok := errors.As(err, &h)
	return h, ok
}
