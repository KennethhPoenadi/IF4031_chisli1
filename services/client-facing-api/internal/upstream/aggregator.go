package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	base string
	key  string
	http *http.Client
	log  *slog.Logger
}

// new creates an Aggregator client whose connection pool matches the API request limit, reusing these connections avoids opening a new TCP connection for every request
func New(base, key string, timeout time.Duration, maxConnections int, log *slog.Logger) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxConnections
	transport.MaxIdleConnsPerHost = maxConnections
	transport.MaxConnsPerHost = maxConnections
	transport.IdleConnTimeout = 90 * time.Second
	transport.ResponseHeaderTimeout = timeout

	return &Client{
		base: base,
		key:  key,
		log:  log,
		http: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
	}
}

func (c *Client) WithTransport(wrap func(http.RoundTripper) http.RoundTripper) *Client {
	c.http.Transport = wrap(c.http.Transport)
	return c
}

type Result struct {
	Events []map[string]any
	Meta   json.RawMessage
}

type StatusError struct {
	Status int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("aggregator returned HTTP %d", e.Status)
}

// memanggil endpoint internal Aggregator dan mengembalikan status serta body.
func (c *Client) Forward(ctx context.Context, method, pathAndQuery, correlationID string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+pathAndQuery, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-Internal-Key", c.key)
	req.Header.Set("X-Correlation-ID", correlationID)
	req.Header.Set("Accept", "application/json")
	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.log.Warn("upstream call failed", "target", "aggregator", "path", pathAndQuery, "duration_ms", time.Since(start).Milliseconds(), "err", err, "correlation_id", correlationID)
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return 0, nil, err
	}
	c.log.Info("upstream call", "target", "aggregator", "path", pathAndQuery, "status", resp.StatusCode, "duration_ms", time.Since(start).Milliseconds(), "correlation_id", correlationID)
	return resp.StatusCode, body, nil
}

func (c *Client) Events(ctx context.Context, q url.Values, correlationID string) (Result, error) {
	u := c.base + "/hazard-events"
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("X-Internal-Key", c.key)
	req.Header.Set("X-Correlation-ID", correlationID)
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.log.Warn("upstream call failed", "target", "aggregator", "duration_ms", time.Since(start).Milliseconds(), "err", err, "correlation_id", correlationID)
		return Result{}, err
	}
	defer resp.Body.Close()
	c.log.Info("upstream call", "target", "aggregator", "status", resp.StatusCode, "duration_ms", time.Since(start).Milliseconds(), "correlation_id", correlationID)

	if resp.StatusCode != http.StatusOK {
		return Result{}, &StatusError{Status: resp.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return Result{}, err
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return Result{}, fmt.Errorf("empty response from aggregator")
	}

	var res Result
	if body[0] == '[' {
		err = json.Unmarshal(body, &res.Events)
	} else {
		var env struct {
			Data []map[string]any `json:"data"`
			Meta json.RawMessage  `json:"meta"`
		}
		err = json.Unmarshal(body, &env)
		res.Events, res.Meta = env.Data, env.Meta
	}
	if err != nil {
		return Result{}, fmt.Errorf("unreadable aggregator response: %w", err)
	}
	return res, nil
}
