package feed

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Event struct {
	ID         int64
	Type       string
	Data       []byte
	ReceivedAt time.Time
}

type Session struct {
	Group        string    `json:"group"`
	Profile      string    `json:"profile"`
	Speed        float64   `json:"speed"`
	VirtualNow   time.Time `json:"virtual_now"`
	LatestOffset int64     `json:"latest_offset"`
}

type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("feed menjawab HTTP %d: %s", e.Code, e.Body)
}

var (
	ErrHeartbeatTimeout = errors.New("heartbeat tidak datang dalam batas waktu")
	ErrGap              = errors.New("offset feed melompat")
)

type Client struct {
	base  string
	token string
	http  *http.Client
	HeartbeatTimeout time.Duration
	FromInclusive bool
	AllowGap bool
}

func New(base, token string, heartbeatTimeout time.Duration) *Client {
	return &Client{base: strings.TrimRight(base, "/"), token: token, HeartbeatTimeout: heartbeatTimeout,
		http: &http.Client{Transport: http.DefaultTransport}, FromInclusive: true}
}

func (c *Client) WithTransport(rt http.RoundTripper) *Client {
	c.http.Transport = rt
	return c
}

func readError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
}

func (c *Client) Session(ctx context.Context) (Session, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/session", nil)
	if err != nil {
		return Session{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Session{}, readError(resp)
	}
	var s Session
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&s)
	return s, err
}

func (c *Client) Stream(ctx context.Context, last int64, handle func(Event) error) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	url := c.base + "/v1/stream"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return last, err
	}
	q := req.URL.Query()
	switch {
	case last >= 0:
		req.Header.Set("Last-Event-ID", strconv.FormatInt(last, 10))
		q.Set("from", strconv.FormatInt(last, 10))
	case c.FromInclusive:
		q.Set("from", "0")
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.http.Do(req)
	if err != nil {
		return last, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return last, readError(resp)
	}

	var mu sync.Mutex
	lastSeen := time.Now()
	timedOut := make(chan struct{})
	go func() {
		t := time.NewTicker(c.HeartbeatTimeout / 4)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				mu.Lock()
				idle := time.Since(lastSeen)
				mu.Unlock()
				if idle > c.HeartbeatTimeout {
					close(timedOut)
					cancel()
					return
				}
			}
		}
	}()

	reader := bufio.NewReaderSize(resp.Body, 64<<10)
	var (
		id      int64 = -1
		typ     string
		data    bytes.Buffer
		hasData bool
	)
	dispatch := func() error {
		defer func() { id, typ, hasData = -1, "", false; data.Reset() }()
		if !hasData && typ == "" {
			return nil
		}
		if typ == "" {
			typ = "message"
		}
		if id < 0 {
			return handle(Event{ID: -1, Type: typ, Data: append([]byte(nil), data.Bytes()...), ReceivedAt: time.Now().UTC()})
		}
		if id <= last {
			return nil // sudah pernah diterima
		}
		if last >= 0 && id != last+1 {
			if !c.AllowGap {
				return fmt.Errorf("%w: harap %d, datang %d", ErrGap, last+1, id)
			}
			c.AllowGap = false
		}
		if err := handle(Event{ID: id, Type: typ, Data: append([]byte(nil), data.Bytes()...), ReceivedAt: time.Now().UTC()}); err != nil {
			return err
		}
		last = id
		return nil
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			mu.Lock()
			lastSeen = time.Now()
			mu.Unlock()
		}
		line = bytes.TrimRight(line, "\r\n")
		if err == nil || len(line) > 0 {
			switch {
			case len(line) == 0:
				if derr := dispatch(); derr != nil {
					return last, derr
				}
			case line[0] == ':':
			default:
				field, value, _ := bytes.Cut(line, []byte(":"))
				value = bytes.TrimPrefix(value, []byte(" "))
				switch string(field) {
				case "id":
					if n, perr := strconv.ParseInt(string(value), 10, 64); perr == nil {
						id = n
					}
				case "event":
					typ = string(value)
				case "data":
					if hasData {
						data.WriteByte('\n')
					}
					data.Write(value)
					hasData = true
				}
			}
		}
		if err != nil {
			select {
			case <-timedOut:
				return last, ErrHeartbeatTimeout
			default:
			}
			if ctx.Err() != nil && !errors.Is(err, io.EOF) {
				return last, ctx.Err()
			}
			return last, fmt.Errorf("koneksi feed terputus: %w", err)
		}
	}
}
