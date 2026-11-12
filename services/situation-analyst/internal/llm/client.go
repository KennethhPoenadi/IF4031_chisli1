package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"telemetry"
)

type Outcome string

const (
	OutcomeOK          Outcome = "ok"
	OutcomeError       Outcome = "error"
	OutcomeTimeout     Outcome = "timeout"
	OutcomeBreakerOpen Outcome = "breaker_open"
	OutcomeBudget      Outcome = "budget"
	OutcomeDisabled    Outcome = "disabled"
	OutcomeBadResponse Outcome = "bad_response"
)

type Config struct {
	BaseURL          string
	Model            string
	APIKey           string
	Timeout          time.Duration
	MaxTokens        int
	MaxPerHour       int
	FailureThreshold int
	OpenFor          time.Duration
	MaxResponseBytes int64
	Transport        http.RoundTripper
}

func (c *Config) defaults() {
	if c.Timeout <= 0 {
		c.Timeout = 20 * time.Second
	}
	if c.MaxTokens <= 0 {
		c.MaxTokens = 300
	}
	if c.MaxPerHour <= 0 {
		c.MaxPerHour = 20
	}
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 3
	}
	if c.OpenFor <= 0 {
		c.OpenFor = time.Minute
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = 64 << 10
	}
}

var (
	requests = telemetry.Counter("llm_requests", "percobaan memanggil LLM, per hasil")
	duration = telemetry.Histogram("llm_request_duration", "detik yang dihabiskan satu panggilan LLM")
)

type Client struct {
	cfg      Config
	endpoint string
	http     *http.Client
	now      func() time.Time
	log      *slog.Logger
	brk      *breaker
	bud      *budget
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func New(cfg Config, now func() time.Time, log *slog.Logger) (*Client, error) {
	cfg.defaults()
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	c := &Client{cfg: cfg, now: now, log: log, brk: newBreaker(cfg.FailureThreshold, cfg.OpenFor), bud: newBudget(cfg.MaxPerHour)}

	if cfg.BaseURL != "" {
		u, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
		if err != nil || u.Host == "" {
			return nil, errors.New("LLM_BASE_URL tidak valid")
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("LLM_BASE_URL tidak boleh memuat kredensial, query, atau fragment")
		}
		if u.Scheme != "https" && (u.Scheme != "http" || !isLoopback(u.Hostname())) {
			return nil, errors.New("LLM_BASE_URL harus berskema https")
		}
		c.endpoint = u.String() + "/chat/completions"
	}
	rt := cfg.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	c.http = &http.Client{
		Timeout:       cfg.Timeout,
		Transport:     rt,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return c, nil
}

func (c *Client) Enabled() bool {
	return c.endpoint != "" && c.cfg.Model != "" && c.cfg.APIKey != ""
}

func (c *Client) BreakerOpen() bool { return c.brk.isOpen(c.now()) }

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func (c *Client) Summarize(ctx context.Context, system, user string) (string, Outcome, error) {
	start := c.now()
	text, outcome, err := c.call(ctx, system, user)
	requests(ctx, 1, "outcome", string(outcome))
	if outcome != OutcomeDisabled && outcome != OutcomeBreakerOpen && outcome != OutcomeBudget {
		duration(ctx, c.now().Sub(start).Seconds(), "outcome", string(outcome))
	}
	return text, outcome, err
}

func (c *Client) call(ctx context.Context, system, user string) (string, Outcome, error) {
	if !c.Enabled() {
		return "", OutcomeDisabled, errors.New("LLM tidak dikonfigurasi")
	}
	now := c.now()
	ok, probe := c.brk.allow(now)
	if !ok {
		return "", OutcomeBreakerOpen, errors.New("circuit breaker LLM terbuka")
	}
	if !c.bud.take(now) {
		if probe {
			c.brk.cancelProbe()
		}
		return "", OutcomeBudget, errors.New("anggaran panggilan LLM per jam habis")
	}

	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	ctx, endSpan := telemetry.StartSpan(ctx, "llm.chat", "llm.model", c.cfg.Model)
	text, outcome, err := c.do(ctx, system, user)
	endSpan(err)

	if outcome == OutcomeOK {
		c.brk.success()
	} else {
		c.brk.failure(c.now())
		c.log.Warn("panggilan LLM gagal", "outcome", string(outcome), "breaker_open", c.brk.isOpen(c.now()))
	}
	return text, outcome, err
}

func (c *Client) do(ctx context.Context, system, user string) (string, Outcome, error) {
	body, err := json.Marshal(chatRequest{
		Model:       c.cfg.Model,
		Messages:    []chatMessage{{Role: "system", Content: system}, {Role: "user", Content: user}},
		MaxTokens:   c.cfg.MaxTokens,
		Temperature: 0.2,
	})
	if err != nil {
		return "", OutcomeError, errors.New("gagal menyusun permintaan")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", OutcomeError, errors.New("gagal membuat permintaan")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)

	resp, err := c.http.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return "", OutcomeTimeout, errors.New("panggilan LLM melewati batas waktu")
		}
		return "", OutcomeError, fmt.Errorf("panggilan LLM gagal: %w", stripURL(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", OutcomeError, fmt.Errorf("LLM menjawab status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, c.cfg.MaxResponseBytes+1))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return "", OutcomeTimeout, errors.New("pembacaan jawaban LLM melewati batas waktu")
		}
		return "", OutcomeError, errors.New("gagal membaca jawaban LLM")
	}
	if int64(len(raw)) > c.cfg.MaxResponseBytes {
		return "", OutcomeBadResponse, errors.New("jawaban LLM terlalu besar")
	}
	var out chatResponse
	if err := json.Unmarshal(raw, &out); err != nil || len(out.Choices) == 0 || strings.TrimSpace(out.Choices[0].Message.Content) == "" {
		return "", OutcomeBadResponse, errors.New("jawaban LLM tidak berbentuk yang diharapkan")
	}
	return out.Choices[0].Message.Content, OutcomeOK, nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

type breaker struct {
	mu        sync.Mutex
	threshold int
	openFor   time.Duration
	failures  int
	state     int
	openedAt  time.Time
	probing   bool
}

func newBreaker(threshold int, openFor time.Duration) *breaker {
	return &breaker{threshold: threshold, openFor: openFor}
}

func (b *breaker) allow(now time.Time) (ok, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case 0:
		return true, false
	case 1:
		if now.Sub(b.openedAt) >= b.openFor {
			b.state, b.probing = 2, true
			return true, true
		}
		return false, false
	default:
		if b.probing {
			return false, false
		}
		b.probing = true
		return true, true
	}
}

func (b *breaker) cancelProbe() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == 2 {
		b.probing = false
	}
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures, b.state, b.probing = 0, 0, false
}

func (b *breaker) failure(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == 2 {
		b.state, b.openedAt, b.probing = 1, now, false
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state, b.openedAt = 1, now
	}
}

func (b *breaker) isOpen(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == 1 && now.Sub(b.openedAt) < b.openFor || b.state == 2
}

type budget struct {
	mu    sync.Mutex
	max   int
	times []time.Time
}

func newBudget(max int) *budget { return &budget{max: max} }

func (b *budget) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cut := now.Add(-time.Hour)
	i := 0
	for i < len(b.times) && !b.times[i].After(cut) {
		i++
	}
	b.times = b.times[i:]
	if len(b.times) >= b.max {
		return false
	}
	b.times = append(b.times, now)
	return true
}
