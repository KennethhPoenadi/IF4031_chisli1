package pantaiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrResponseTooLarge = errors.New("pantai response exceeds configured limit")

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("pantai returned HTTP %d", e.StatusCode)
}

type Config struct {
	BaseURL    string
	Token      string
	Timeout    time.Duration
	MaxBody    int64
	PageLimit  int
	HTTPClient *http.Client
}

type Page struct {
	GeneratedAt  string
	Observations []map[string]any
	NextCursor   string
}

type StationPage struct {
	GeneratedAt string
	Stations    []map[string]any
}

type Client struct {
	baseURL   string
	token     string
	maxBody   int64
	pageLimit int
	timeout   time.Duration
	http      *http.Client
}

func New(cfg Config) (*Client, error) {
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.BaseURL == "" {
		return nil, errors.New("pantai base URL is required")
	}
	baseURL, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid pantai base URL: %w", err)
	}
	if (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return nil, errors.New("pantai base URL must use http or https and include a host")
	}
	if baseURL.User != nil {
		return nil, errors.New("pantai base URL must not contain credentials")
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, errors.New("pantai base URL must not contain a query or fragment")
	}
	cfg.Token = strings.TrimSpace(cfg.Token)
	if cfg.Token == "" {
		return nil, errors.New("pantai token is required")
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 2 << 20
	}
	if cfg.PageLimit <= 0 {
		cfg.PageLimit = 500
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.Timeout}
	}
	return &Client{
		baseURL: cfg.BaseURL, token: cfg.Token, maxBody: cfg.MaxBody,
		pageLimit: cfg.PageLimit, timeout: cfg.Timeout, http: cfg.HTTPClient,
	}, nil
}

func (c *Client) Fetch(ctx context.Context, cursor string) (Page, error) {
	q := make(url.Values)
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	q.Set("limit", fmt.Sprint(c.pageLimit))
	body, err := c.get(ctx, "/v1/observations", q)
	if err != nil {
		return Page{}, err
	}
	var wire struct {
		GeneratedAt string           `json:"generated_at"`
		Items       []map[string]any `json:"items"`
		NextCursor  string           `json:"next_cursor"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&wire); err != nil {
		return Page{}, fmt.Errorf("decode pantai page: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return Page{}, errors.New("decode pantai page: trailing JSON data")
	}
	if wire.Items == nil {
		return Page{}, errors.New("decode pantai page: items must be an array")
	}
	if _, err := time.Parse(time.RFC3339, wire.GeneratedAt); err != nil {
		return Page{}, fmt.Errorf("decode pantai page: generated_at must be RFC3339: %w", err)
	}
	if len(wire.Items) > c.pageLimit {
		return Page{}, fmt.Errorf("pantai page has %d items; limit is %d", len(wire.Items), c.pageLimit)
	}
	return Page{GeneratedAt: wire.GeneratedAt, Observations: wire.Items, NextCursor: wire.NextCursor}, nil
}

func (c *Client) Stations(ctx context.Context) (StationPage, error) {
	body, err := c.get(ctx, "/v1/stations", nil)
	if err != nil {
		return StationPage{}, err
	}
	var wire struct {
		GeneratedAt string           `json:"generated_at"`
		Items       []map[string]any `json:"items"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&wire); err != nil {
		return StationPage{}, fmt.Errorf("decode pantai stations: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return StationPage{}, errors.New("decode pantai stations: trailing JSON data")
	}
	if wire.Items == nil {
		return StationPage{}, errors.New("decode pantai stations: items must be an array")
	}
	if wire.GeneratedAt != "" {
		if _, err := time.Parse(time.RFC3339, wire.GeneratedAt); err != nil {
			return StationPage{}, fmt.Errorf("decode pantai stations: generated_at must be RFC3339: %w", err)
		}
	}
	return StationPage{GeneratedAt: wire.GeneratedAt, Stations: wire.Items}, nil
}

func (c *Client) get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	u, err := url.Parse(c.baseURL + path)
	if err != nil {
		return nil, err
	}
	if query != nil {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, &HTTPError{StatusCode: resp.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, ErrResponseTooLarge
	}
	return body, nil
}
