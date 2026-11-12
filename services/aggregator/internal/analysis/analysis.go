package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"aggregator/internal/httpx"
)

const (
	maxBody        = 64 << 10
	defaultTimeout = 2 * time.Second
)

type Client struct {
	endpoint string
	key      string
	http     *http.Client
}

func New(baseURL, internalKey string, timeout time.Duration, rt http.RoundTripper) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil
	}
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("SITUATION_ANALYST_URL tidak valid")
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if rt == nil {
		rt = http.DefaultTransport
	}
	return &Client{
		endpoint: u.String() + "/analysis/latest",
		key:      internalKey,
		http: &http.Client{Timeout: timeout, Transport: rt,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}

func (c *Client) Latest(ctx context.Context) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return nil, errors.New("gagal membuat permintaan")
	}
	req.Header.Set("X-Internal-Key", c.key)
	req.Header.Set("Accept", "application/json")
	if id := httpx.CorrelationID(ctx); id != "" {
		req.Header.Set("X-Correlation-ID", id)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("situation-analyst tidak terjangkau: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("situation-analyst menjawab status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, errors.New("gagal membaca jawaban situation-analyst")
	}
	if len(raw) > maxBody {
		return nil, errors.New("jawaban situation-analyst terlalu besar")
	}
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, errors.New("jawaban situation-analyst bukan JSON objek")
	}
	return raw, nil
}
