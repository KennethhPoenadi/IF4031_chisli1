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
	"sync"
	"time"

	"client-facing-api/internal/httpx"
)

type Client struct {
	base string
	key  string
	http *http.Client
	log  *slog.Logger

	// Panggilan Events yang sedang berjalan, dikelompokkan menurut kueri. Lihat Events.
	mu       sync.Mutex
	inflight map[string]*call
}

// call adalah satu panggilan ke Aggregator yang hasilnya dibagi ke semua penunggu kueri yang sama.
type call struct {
	done chan struct{}
	res  Result
	err  error
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

// Events mengembalikan kejadian dari Aggregator. Permintaan dengan kueri yang sama yang tiba selagi satu panggilan masih berjalan
// digabung menjadi satu panggilan (request coalescing) dan membagi hasilnya. Pada beban tinggi ratusan klien meminta kueri yang
// sama persis, dan tanpa penggabungan tiap permintaan memicu pemindaian penuh ke semua unit penyimpanan (sekitar 15 ms CPU Mongo
// dan 8 ms CPU Aggregator per permintaan), sehingga CPU habis dan permintaan berujung timeout.
//
// Aman terhadap hak akses: kueri hanya berisi parameter filter (limit, hazard_type, severity, since) dan tidak memuat identitas
// klien; pembatasan field per peran dilakukan pemanggil sesudah hasil kembali. Tidak ada cache berumur: hasil hanya dibagi ke
// permintaan yang menunggu panggilan yang sedang berjalan, jadi data sesegar satu panggilan biasa. Hasil dibaca bersama dan tidak
// boleh diubah oleh pemanggil. Pemanggil yang batal (context) berhenti menunggu tanpa membatalkan panggilan untuk yang lain.
func (c *Client) Events(ctx context.Context, q url.Values, correlationID string) (Result, error) {
	// Kelas prioritas ikut kunci: permintaan prioritas hanya digabung dengan sesama prioritas, jadi panggilan yang macet milik
	// permintaan biasa tidak pernah menahan permintaan prioritas (isolasi prioritas P5).
	key := q.Encode()
	if httpx.IsPriority(ctx) {
		key = "priority|" + key
	}
	c.mu.Lock()
	if cl, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		return cl.wait(ctx)
	}
	cl := &call{done: make(chan struct{})}
	if c.inflight == nil {
		c.inflight = make(map[string]*call)
	}
	c.inflight[key] = cl
	c.mu.Unlock()

	// Panggilan berjalan terpisah dari pembatalan pemanggil pertama; batas waktunya tetap dari http.Client.Timeout.
	go func() {
		cl.res, cl.err = c.fetchEvents(context.WithoutCancel(ctx), q, correlationID)
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(cl.done)
	}()
	return cl.wait(ctx)
}

func (cl *call) wait(ctx context.Context) (Result, error) {
	select {
	case <-cl.done:
		return cl.res, cl.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (c *Client) fetchEvents(ctx context.Context, q url.Values, correlationID string) (Result, error) {
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
