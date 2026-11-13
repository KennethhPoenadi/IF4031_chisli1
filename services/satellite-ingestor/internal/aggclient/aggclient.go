package aggclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"satellite-ingestor/internal/hazard"
)

type Detection struct {
	Key    string          `json:"key"`
	Offset int64           `json:"offset"`
	Final  bool            `json:"final"`
	Data   json.RawMessage `json:"data"`
}

type Batch struct {
	LeaseToken       string            `json:"lease_token,omitempty"`
	Offset           int64             `json:"offset"`
	MaxEventTime     time.Time         `json:"max_event_time"`
	VirtualNow       time.Time         `json:"virtual_now"`
	PutDetections    []Detection       `json:"put_detections"`
	DeleteDetections []string          `json:"delete_detections"`
	Late             []Detection       `json:"late"`
	UpsertFires      []hazard.Event    `json:"upsert_fires"`
	DeleteFires      []string          `json:"delete_fires"`
	ActiveFires      map[string]string `json:"active_fires"`
	Stats            map[string]int64  `json:"stats"`
}

type Checkpoint struct {
	Offset       int64             `json:"offset"`
	MaxEventTime time.Time         `json:"max_event_time"`
	VirtualNow   time.Time         `json:"virtual_now"`
	ActiveFires  map[string]string `json:"active_fires"`
	Stats        map[string]int64  `json:"stats"`
}

type HTTPError struct {
	Code int
	Body string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("aggregator HTTP %d: %s", e.Code, e.Body) }

type Client struct {
	base string
	key  string
	http *http.Client
}

func New(base, key string) *Client {
	return &Client{base: strings.TrimRight(base, "/"), key: key, http: &http.Client{Timeout: 90 * time.Second}}
}

func (c *Client) WithTransport(rt http.RoundTripper) *Client {
	c.http.Transport = rt
	return c
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("X-Internal-Key", c.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return &HTTPError{Code: resp.StatusCode, Body: strings.TrimSpace(string(data[:min(len(data), 512)]))}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (c *Client) Apply(ctx context.Context, b Batch) error {
	return c.do(ctx, http.MethodPost, "/internal/ingest/satellite/batch", b, nil)
}

func (c *Client) Checkpoint(ctx context.Context) (*Checkpoint, error) {
	var out struct {
		Found      bool        `json:"found"`
		Checkpoint *Checkpoint `json:"checkpoint"`
	}
	if err := c.do(ctx, http.MethodGet, "/internal/ingest/satellite/checkpoint", nil, &out); err != nil {
		return nil, err
	}
	if !out.Found {
		return nil, nil
	}
	return out.Checkpoint, nil
}

func (c *Client) ActiveDetections(ctx context.Context, maxOffset int64) ([]Detection, bool, error) {
	var out struct {
		Items   []Detection `json:"items"`
		Partial bool        `json:"partial"`
	}
	err := c.do(ctx, http.MethodGet, "/internal/ingest/satellite/detections?max_offset="+strconv.FormatInt(maxOffset, 10), nil, &out)
	return out.Items, out.Partial, err
}
