package pvmbgclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"time"
	"unicode/utf8"
)

const (
	MaxBodyBytes = 8 << 20
	MaxElementBytes = 64 << 10
	MaxDepth = 32
)

type Item struct {
	Raw    []byte
	Report map[string]any
}

type Broken struct {
	Raw    []byte
	Reason string
}


type Batch struct {
	Items  []Item
	Broken []Broken
	Truncated bool
}

func DecodeBatch(body []byte, overLimit bool) Batch {
	var b Batch
	i := skipWS(body, 0)
	if i >= len(body) {
		if overLimit {
			b.Truncated = true
		}
		return b
	}
	if body[i] != '[' {
		b.Broken = append(b.Broken, Broken{Raw: clip(body), Reason: "malformed: body bukan array JSON"})
		b.Truncated = overLimit
		return b
	}
	i++
	for {
		i = skipWS(body, i)
		if i >= len(body) {
			b.Truncated = true
			if overLimit {
				b.Broken = append(b.Broken, Broken{Raw: nil, Reason: fmt.Sprintf("malformed: body melebihi batas %d byte", MaxBodyBytes)})
			}
			return b
		}
		if body[i] == ']' {
			return b
		}
		if body[i] == ',' {
			i++
			continue
		}
		end, depth, complete := scanValue(body, i)
		if end == i {
			b.Broken = append(b.Broken, Broken{Raw: clip(body[i : i+1]), Reason: "malformed: JSON tidak valid: penutup tanpa pembuka"})
			i++
			continue
		}
		raw := body[i:end]
		if !complete {
			b.Truncated = true
			reason := "malformed: JSON terpotong"
			if overLimit {
				reason = fmt.Sprintf("malformed: body melebihi batas %d byte dan terpotong", MaxBodyBytes)
			}
			b.Broken = append(b.Broken, Broken{Raw: clip(raw), Reason: reason})
			return b
		}
		b.add(raw, depth)
		i = end
	}
}

func (b *Batch) add(raw []byte, depth int) {
	switch {
	case len(raw) > MaxElementBytes:
		b.Broken = append(b.Broken, Broken{Raw: clip(raw), Reason: fmt.Sprintf("malformed: ukuran report %d byte melebihi batas %d byte", len(raw), MaxElementBytes)})
		return
	case !utf8.Valid(raw):
		b.Broken = append(b.Broken, Broken{Raw: clip(raw), Reason: "malformed: UTF-8 tidak valid"})
		return
	case depth > MaxDepth:
		b.Broken = append(b.Broken, Broken{Raw: clip(raw), Reason: fmt.Sprintf("malformed: nesting terlalu dalam (%d > %d)", depth, MaxDepth)})
		return
	}
	if raw[0] != '{' {
		b.Broken = append(b.Broken, Broken{Raw: clip(raw), Reason: "malformed: report bukan objek JSON"})
		return
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		reason := "malformed: report bukan objek JSON"
		if err != nil {
			reason = "malformed: JSON tidak valid: " + err.Error()
		}
		b.Broken = append(b.Broken, Broken{Raw: clip(raw), Reason: reason})
		return
	}
	b.Items = append(b.Items, Item{Raw: append([]byte(nil), raw...), Report: m})
}

func clip(raw []byte) []byte {
	const keep = MaxElementBytes
	if len(raw) > keep {
		return append([]byte(nil), raw[:keep]...)
	}
	return append([]byte(nil), raw...)
}

func skipWS(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t') {
		i++
	}
	return i
}

func scanValue(b []byte, i int) (end, maxDepth int, complete bool) {
	depth := 0
	inString := false
	for j := i; j < len(b); j++ {
		c := b[j]
		if inString {
			switch c {
			case '\\':
				j++
			case '"':
				inString = false
				if depth == 0 {
					return j + 1, maxDepth, true
				}
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case '}', ']':
			if depth == 0 {
				return j, maxDepth, true
			}
			depth--
			if depth == 0 {
				return j + 1, maxDepth, true
			}
		case ',':
			if depth == 0 {
				return j, maxDepth, true
			}
		}
	}
	return len(b), maxDepth, false
}

// VolcanicBatch seperti VolcanicReports, tetapi membaca respons secara toleran.
func (c *Client) VolcanicBatch(ctx context.Context, since string, correlationID string) (Batch, error) {
	url := c.baseURL + "/volcanic-reports"
	if since != "" {
		url += "?since=" + neturl.QueryEscape(since)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Batch{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Correlation-ID", correlationID)

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.log.Warn("pvmbg call failed", "duration_ms", time.Since(start).Milliseconds(), "err", err, "correlation_id", correlationID)
		return Batch{}, err
	}
	defer resp.Body.Close()
	c.log.Info("pvmbg call", "status", resp.StatusCode, "duration_ms", time.Since(start).Milliseconds(), "correlation_id", correlationID)

	if resp.StatusCode == http.StatusServiceUnavailable {
		return Batch{}, ErrOutage
	}
	if resp.StatusCode != http.StatusOK {
		return Batch{}, fmt.Errorf("pvmbg returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return Batch{}, err
	}
	over := len(body) > MaxBodyBytes
	if over {
		body = body[:MaxBodyBytes]
	}
	return DecodeBatch(bytes.TrimSpace(body), over), nil
}
