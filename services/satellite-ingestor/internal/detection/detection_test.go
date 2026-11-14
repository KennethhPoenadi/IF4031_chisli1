package detection

import (
	"errors"
	"testing"
	"time"
)

func TestParseKeepsRawTextInKey(t *testing.T) {
	d, err := Parse([]byte(`{"latitude":-2.51230,"longitude":113.80440,"acq_date":"2026-11-12","acq_time":243,"satellite":"N20","confidence":"h","frp":5.25}`))
	if err != nil {
		t.Fatal(err)
	}
	if d.Key != "N20|2026-11-12|243|-2.51230|113.80440" {
		t.Fatalf("key = %q (koordinat tidak boleh dibulatkan atau diformat ulang)", d.Key)
	}
	if !d.Time.Equal(time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC)) || d.Confidence != "h" || d.FRP != 5.25 {
		t.Fatalf("%+v", d)
	}
}

func TestParseAcceptsStringsAndWrappedPayload(t *testing.T) {
	d, err := Parse([]byte(`{"detection":{"latitude":"-1.5","longitude":"110.25","acq_date":"2026-11-12","acq_time":"0005","satellite":"N21","confidence":"low"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if d.Key != "N21|2026-11-12|0005|-1.5|110.25" || !d.Low() || d.Time.Minute() != 5 {
		t.Fatalf("%+v", d)
	}
}

func TestConfidenceNormalization(t *testing.T) {
	for raw, want := range map[string]string{"l": "l", "nominal": "n", "H": "h", "15": "l", "55": "n", "95": "h"} {
		if got := normalizeConfidence(raw); got != want {
			t.Fatalf("%q -> %q, ingin %q", raw, got, want)
		}
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	for _, body := range []string{
		`{`,
		`{"latitude":1,"longitude":2,"acq_date":"2026-11-12","acq_time":243}`,
		`{"latitude":91,"longitude":2,"acq_date":"2026-11-12","acq_time":243,"satellite":"N20"}`,
		`{"latitude":1,"longitude":2,"acq_date":"12/11/2026","acq_time":243,"satellite":"N20"}`,
		`{"latitude":1,"longitude":2,"acq_date":"2026-11-12","acq_time":"2a3","satellite":"N20"}`,
	} {
		if _, err := Parse([]byte(body)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Parse(%s) err=%v", body, err)
		}
	}
}

func TestParseKeyForRetract(t *testing.T) {
	k1, err := ParseKey([]byte(`{"key":"N20|2026-11-12|243|-2.51230|113.80440"}`))
	if err != nil || k1 != "N20|2026-11-12|243|-2.51230|113.80440" {
		t.Fatalf("%q %v", k1, err)
	}
	k2, err := ParseKey([]byte(`{"satellite":"N20","acq_date":"2026-11-12","acq_time":243,"latitude":-2.51230,"longitude":113.80440}`))
	if err != nil || k2 != k1 {
		t.Fatalf("%q %v", k2, err)
	}
}

func TestVirtualNow(t *testing.T) {
	if ts, ok := VirtualNow([]byte(`{"virtual_now":"2026-11-12T03:00:00Z"}`)); !ok || ts.Hour() != 3 {
		t.Fatal("virtual_now tidak terbaca")
	}
	if _, ok := VirtualNow([]byte(`{}`)); ok {
		t.Fatal("tanpa virtual_now harus false")
	}
}
