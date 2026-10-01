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

func New(base, key string, timeout time.Duration, log *slog.Logger) *Client {
	return &Client{
		base: base,
		key:  key,
		log:  log,
		http: &http.Client{Timeout: timeout},
	}
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
