package pvmbgclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

var ErrOutage = errors.New("pvmbg is in outage mode")

type Client struct {
	baseURL string
	token   string
	http    *http.Client
	log     *slog.Logger
}

func New(baseURL, token string, timeout time.Duration, log *slog.Logger) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		http:    &http.Client{Timeout: timeout},
		log:     log,
	}
}

func (c *Client) VolcanicReports(ctx context.Context, since string, correlationID string) ([]map[string]any, error) {
	url := c.baseURL + "/volcanic-reports"
	if since != "" {
		url += "?since=" + since
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Correlation-ID", correlationID)

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.log.Warn("pvmbg call failed", "duration_ms", time.Since(start).Milliseconds(), "err", err, "correlation_id", correlationID)
		return nil, err
	}
	defer resp.Body.Close()
	c.log.Info("pvmbg call", "status", resp.StatusCode, "duration_ms", time.Since(start).Milliseconds(), "correlation_id", correlationID)

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrOutage
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pvmbg returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}

	var reports []map[string]any
	if err := json.Unmarshal(body, &reports); err != nil {
		return nil, err
	}
	return reports, nil
}
