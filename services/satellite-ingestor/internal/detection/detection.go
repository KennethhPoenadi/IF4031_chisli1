package detection

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Detection struct {
	Key           string    `json:"key"`
	Satellite     string    `json:"satellite"`
	AcqDate       string    `json:"acq_date"`
	AcqTime       string    `json:"acq_time"`
	LatRaw        string    `json:"latitude"`
	LonRaw        string    `json:"longitude"`
	Lat           float64   `json:"lat"`
	Lon           float64   `json:"lon"`
	Time          time.Time `json:"time"`
	Confidence    string    `json:"confidence"` // l, n, atau h
	ConfidenceRaw string    `json:"confidence_raw"`
	FRP           float64   `json:"frp"`
	Offset        int64     `json:"offset"`
	ReceivedAt    time.Time `json:"received_at"`
	// Group adalah kelompok (kebakaran atau deteksi tunggal) tempat deteksi dibekukan
	// ketika selesai dikenali. kosong untuk deteksi yang masih aktif.
	Group string          `json:"group,omitempty"`
	Raw   json.RawMessage `json:"raw,omitempty"`
}

func (d Detection) Low() bool { return d.Confidence == "l" }

var ErrInvalid = errors.New("deteksi tidak valid")

// mengembalikan objek deteksi di dalam payload: payload itu sendiri, atau
// sub-objek "detection"/"data"/"record" apabila feed membungkusnya.
func object(data []byte) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	for _, wrap := range []string{"detection", "data", "record"} {
		if inner, ok := m[wrap]; ok && len(inner) > 0 && inner[0] == '{' {
			var im map[string]json.RawMessage
			if err := json.Unmarshal(inner, &im); err == nil {
				return im, nil
			}
		}
	}
	return m, nil
}

func text(m map[string]json.RawMessage, names ...string) (string, bool) {
	for _, n := range names {
		v, ok := m[n]
		if !ok {
			continue
		}
		v = bytes.TrimSpace(v)
		if len(v) == 0 || string(v) == "null" {
			continue
		}
		if v[0] == '"' {
			var s string
			if json.Unmarshal(v, &s) == nil {
				return strings.TrimSpace(s), true
			}
			continue
		}
		return string(v), true
	}
	return "", false
}

func NormalizeAcqTime(s string) (string, error) {
	s = strings.ReplaceAll(strings.TrimSpace(s), ":", "")
	if s == "" || len(s) > 4 {
		return "", fmt.Errorf("acq_time %q tidak valid", s)
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("acq_time %q tidak valid", s)
		}
	}
	return strings.Repeat("0", 4-len(s)) + s, nil
}

func normalizeConfidence(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "l", "low":
		return "l"
	case "n", "nominal", "medium":
		return "n"
	case "h", "high":
		return "h"
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		switch {
		case f < 30:
			return "l"
		case f < 80:
			return "n"
		default:
			return "h"
		}
	}
	return "n"
}

func Key(satellite, acqDate, acqTime, lat, lon string) string {
	return satellite + "|" + acqDate + "|" + acqTime + "|" + lat + "|" + lon
}

func Parse(data []byte) (Detection, error) {
	m, err := object(data)
	if err != nil {
		return Detection{}, err
	}
	var d Detection
	var ok bool
	if d.Satellite, ok = text(m, "satellite"); !ok {
		return Detection{}, fmt.Errorf("%w: satellite tidak ada", ErrInvalid)
	}
	if d.AcqDate, ok = text(m, "acq_date"); !ok {
		return Detection{}, fmt.Errorf("%w: acq_date tidak ada", ErrInvalid)
	}
	if d.AcqTime, ok = text(m, "acq_time"); !ok {
		return Detection{}, fmt.Errorf("%w: acq_time tidak ada", ErrInvalid)
	}
	if d.LatRaw, ok = text(m, "latitude", "lat"); !ok {
		return Detection{}, fmt.Errorf("%w: latitude tidak ada", ErrInvalid)
	}
	if d.LonRaw, ok = text(m, "longitude", "lon"); !ok {
		return Detection{}, fmt.Errorf("%w: longitude tidak ada", ErrInvalid)
	}
	if d.Lat, err = strconv.ParseFloat(d.LatRaw, 64); err != nil || d.Lat < -90 || d.Lat > 90 {
		return Detection{}, fmt.Errorf("%w: latitude %q", ErrInvalid, d.LatRaw)
	}
	if d.Lon, err = strconv.ParseFloat(d.LonRaw, 64); err != nil || d.Lon < -180 || d.Lon > 180 {
		return Detection{}, fmt.Errorf("%w: longitude %q", ErrInvalid, d.LonRaw)
	}
	hhmm, err := NormalizeAcqTime(d.AcqTime)
	if err != nil {
		return Detection{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	d.Time, err = time.ParseInLocation("2006-01-02 1504", d.AcqDate+" "+hhmm, time.UTC)
	if err != nil {
		return Detection{}, fmt.Errorf("%w: acq_date %q", ErrInvalid, d.AcqDate)
	}
	d.ConfidenceRaw, _ = text(m, "confidence")
	d.Confidence = normalizeConfidence(d.ConfidenceRaw)
	if frp, ok := text(m, "frp"); ok {
		d.FRP, _ = strconv.ParseFloat(frp, 64)
	}
	d.Key = Key(d.Satellite, d.AcqDate, d.AcqTime, d.LatRaw, d.LonRaw)
	d.Raw = append(json.RawMessage(nil), data...)
	return d, nil
}

func ParseKey(data []byte) (string, error) {
	m, err := object(data)
	if err != nil {
		return "", err
	}
	if k, ok := text(m, "key", "detection_key", "id"); ok && strings.Count(k, "|") == 4 {
		return k, nil
	}
	sat, ok1 := text(m, "satellite")
	date, ok2 := text(m, "acq_date")
	at, ok3 := text(m, "acq_time")
	lat, ok4 := text(m, "latitude", "lat")
	lon, ok5 := text(m, "longitude", "lon")
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return "", fmt.Errorf("%w: kunci deteksi tidak dapat disusun", ErrInvalid)
	}
	return Key(sat, date, at, lat, lon), nil
}

func VirtualNow(data []byte) (time.Time, bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal(data, &m) != nil {
		return time.Time{}, false
	}
	s, ok := text(m, "virtual_now", "virtual_time", "now")
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}
