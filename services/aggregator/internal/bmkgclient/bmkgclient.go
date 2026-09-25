package bmkgclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	log     *slog.Logger
}

func New(baseURL, apiKey string, timeout time.Duration, log *slog.Logger) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
		log:     log,
	}
}

func (c *Client) SeismicEvents(ctx context.Context, since string, correlationID string) ([]map[string]any, error) {
	var events []map[string]any
	err := c.get(ctx, "/seismic-events", since, correlationID, &events)
	return events, err
}

func (c *Client) TsunamiWarnings(ctx context.Context, since string, correlationID string) ([]map[string]any, error) {
	var warnings []map[string]any
	err := c.get(ctx, "/tsunami-warnings", since, correlationID, &warnings)
	return warnings, err
}

func (c *Client) get(ctx context.Context, path, since, correlationID string, out any) error {
	url := c.baseURL + path
	if since != "" {
		url += "?since=" + since
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-BMKG-Key", c.apiKey)
	req.Header.Set("X-Correlation-ID", correlationID)

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.log.Warn("bmkg call failed", "path", path, "duration_ms", time.Since(start).Milliseconds(), "err", err, "correlation_id", correlationID)
		return err
	}
	defer resp.Body.Close()
	c.log.Info("bmkg call", "path", path, "status", resp.StatusCode, "duration_ms", time.Since(start).Milliseconds(), "correlation_id", correlationID)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bmkg returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}

	return json.Unmarshal(body, out)
}
