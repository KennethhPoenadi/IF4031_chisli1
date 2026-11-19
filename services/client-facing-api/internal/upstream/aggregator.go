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

	// Cache singkat khusus kelas reguler (bukan prioritas). Lihat WithRegularCache.
	regularTTL time.Duration
	regular    map[string]cached

	// hedgeAfter > 0: panggilan kelas prioritas yang belum kembali dalam waktu ini disusul panggilan kedua (ke replika lain lewat
	// Service); jawaban pertama yang sukses dipakai. Lihat WithPriorityHedge.
	hedgeAfter time.Duration

	// Jawaban sukses terakhir kelas prioritas per kueri, untuk WithPriorityStale.
	staleAfter time.Duration
	maxStale   time.Duration
	lastGood   map[string]lastGood
}

type lastGood struct {
	res Result
	at  time.Time
}

// WithPriorityStale membatasi waktu tunggu kelas prioritas (Tim Lapangan). Bila Aggregator belum menjawab dalam after, atau menjawab
// gagal, permintaan prioritas dijawab dengan hasil sukses terakhir untuk kueri yang sama selama umurnya tidak lebih dari max; hasil
// seperti itu ditandai StaleFor (pemanggil wajib menampilkannya ke klien). Bila tidak ada hasil yang cukup segar, perilakunya sama
// seperti tanpa opsi ini. after <= 0 mematikannya.
func (c *Client) WithPriorityStale(after, max time.Duration) *Client {
	c.staleAfter, c.maxStale = after, max
	return c
}

func (c *Client) stale(key string) (Result, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.lastGood[key]
	if !ok {
		return Result{}, false
	}
	age := time.Since(e.at)
	if age > c.maxStale {
		return Result{}, false
	}
	r := e.res
	r.StaleFor = max(age, time.Millisecond)
	return r, true
}

// waitPriority menunggu panggilan kelas prioritas dengan batas staleAfter; lihat WithPriorityStale.
func (c *Client) waitPriority(ctx context.Context, key string, cl *call) (Result, error) {
	if c.staleAfter <= 0 {
		return cl.wait(ctx)
	}
	t := time.NewTimer(c.staleAfter)
	defer t.Stop()
	select {
	case <-cl.done:
	case <-t.C:
		if r, ok := c.stale(key); ok {
			return r, nil
		}
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	res, err := cl.wait(ctx)
	if err != nil && ctx.Err() == nil {
		if r, ok := c.stale(key); ok {
			return r, nil
		}
	}
	return res, err
}

type cached struct {
	res     Result
	expires time.Time
}

const regularCacheMax = 256

// WithPriorityHedge memangkas ekor latensi kelas prioritas. Permintaan identik digabung (coalescing), jadi satu panggilan lambat ke
// Aggregator menggagalkan semua penunggunya sekaligus. Bila panggilan belum kembali sesudah d, panggilan kedua dikirim (Service
// membaginya ke replika lain) dan jawaban sukses yang pertama dipakai. Hanya kelas prioritas; d <= 0 mematikannya.
func (c *Client) WithPriorityHedge(d time.Duration) *Client {
	c.hedgeAfter = d
	return c
}

// WithRegularCache menyimpan hasil Events untuk kelas reguler (Media) selama ttl. Pada beban Media tinggi ribuan klien meminta kueri
// yang sama; tanpa cache setiap pod tetap memanggil Aggregator tanpa henti walau jawabannya hampir selalu 429 atau sama, dan beban itu
// memperlambat penyimpanan yang juga melayani Tim Lapangan. Kelas prioritas tidak pernah memakai cache sehingga selalu segar; kelas
// reguler paling lama tertinggal ttl. ttl <= 0 mematikan cache.
func (c *Client) WithRegularCache(ttl time.Duration) *Client {
	c.regularTTL = ttl
	return c
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
	// StaleFor > 0: hasil ini adalah jawaban sukses terakhir untuk kueri yang sama, berumur sebesar ini (lihat WithPriorityStale).
	StaleFor time.Duration
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
	priority := httpx.IsPriority(ctx)
	if priority {
		key = "priority|" + key
	}
	c.mu.Lock()
	if !priority && c.regularTTL > 0 {
		if e, ok := c.regular[key]; ok && time.Now().Before(e.expires) {
			c.mu.Unlock()
			return e.res, nil
		}
	}
	if cl, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		if priority {
			return c.waitPriority(ctx, key, cl)
		}
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
		cl.res, cl.err = c.fetchEventsHedged(context.WithoutCancel(ctx), q, correlationID)
		c.mu.Lock()
		delete(c.inflight, key)
		if !priority && c.regularTTL > 0 && cl.err == nil {
			if c.regular == nil || len(c.regular) >= regularCacheMax {
				c.regular = make(map[string]cached)
			}
			c.regular[key] = cached{res: cl.res, expires: time.Now().Add(c.regularTTL)}
		}
		if priority && c.staleAfter > 0 && cl.err == nil {
			if c.lastGood == nil || len(c.lastGood) >= regularCacheMax {
				c.lastGood = make(map[string]lastGood)
			}
			c.lastGood[key] = lastGood{res: cl.res, at: time.Now()}
		}
		c.mu.Unlock()
		close(cl.done)
	}()
	if priority {
		return c.waitPriority(ctx, key, cl)
	}
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

func (c *Client) fetchEventsHedged(ctx context.Context, q url.Values, correlationID string) (Result, error) {
	if c.hedgeAfter <= 0 || !httpx.IsPriority(ctx) {
		return c.fetchEvents(ctx, q, correlationID)
	}
	type out struct {
		res Result
		err error
	}
	hctx, cancel := context.WithCancel(ctx)
	defer cancel() // panggilan yang kalah dibatalkan
	ch := make(chan out, 2)
	launch := func() {
		go func() {
			res, err := c.fetchEvents(hctx, q, correlationID)
			ch <- out{res, err}
		}()
	}
	launch()
	timer := time.NewTimer(c.hedgeAfter)
	defer timer.Stop()
	pending := 1
	var last out
	for pending > 0 {
		select {
		case x := <-ch:
			pending--
			if x.err == nil {
				return x.res, nil
			}
			last = x
			if pending == 0 && timer.Stop() { // panggilan pertama gagal sebelum waktunya: coba sekali lagi segera
				launch()
				pending++
			}
		case <-timer.C:
			launch()
			pending++
		}
	}
	return last.res, last.err
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
