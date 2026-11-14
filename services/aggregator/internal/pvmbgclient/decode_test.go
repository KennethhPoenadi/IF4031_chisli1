package pvmbgclient

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const good1 = `{"report_id":"pvmbg-1","alert_level":"Siaga"}`
const good2 = `{"report_id":"pvmbg-2","alert_level":"Awas"}`

func TestDecodeBatchKeepsGoodReportsAroundBrokenOnes(t *testing.T) {
	deep := strings.Repeat("[", 5000) + strings.Repeat("]", 5000)
	cases := []struct {
		name      string
		body      string
		items     int
		broken    int
		truncated bool
		reason    string
	}{
		{"semua baik", "[" + good1 + "," + good2 + "]", 2, 0, false, ""},
		{"terpotong", "[" + good1 + "," + good2[:20], 1, 1, true, "terpotong"},
		{"nesting dalam", "[" + good1 + `,{"x":` + deep + `},` + good2 + "]", 2, 1, false, "nesting"},
		{"utf8 tidak valid", "[" + good1 + ",{\"report_id\":\"\xff\xfe\"}," + good2 + "]", 2, 1, false, "UTF-8"},
		{"ukuran berlebih", "[" + good1 + `,{"pad":"` + strings.Repeat("a", MaxElementBytes) + `"},` + good2 + "]", 2, 1, false, "ukuran"},
		{"bukan objek", "[" + good1 + ",42," + good2 + "]", 2, 1, false, "bukan objek"},
		{"bukan array", `{"error":"x"}`, 0, 1, false, "bukan array"},
		{"kosong", "[]", 0, 0, false, ""},
		{"penutup nyasar", "[" + good1 + ",}," + good2 + "]", 2, 1, false, "penutup"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := DecodeBatch([]byte(tc.body), false)
			if len(b.Items) != tc.items || len(b.Broken) != tc.broken || b.Truncated != tc.truncated {
				t.Fatalf("items=%d broken=%d truncated=%v, ingin %d/%d/%v (%+v)", len(b.Items), len(b.Broken), b.Truncated, tc.items, tc.broken, tc.truncated, b.Broken)
			}
			if tc.reason != "" && !strings.Contains(b.Broken[0].Reason, tc.reason) {
				t.Fatalf("alasan %q tidak memuat %q", b.Broken[0].Reason, tc.reason)
			}
		})
	}
}

func TestDecodeBatchOverLimitIsMarked(t *testing.T) {
	b := DecodeBatch([]byte("["+good1+","+good2[:10]), true)
	if !b.Truncated || len(b.Broken) != 1 || !strings.Contains(b.Broken[0].Reason, "melebihi batas") {
		t.Fatalf("%+v", b)
	}
}

func TestVolcanicBatchReadsTolerantly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "["+good1+",{\"broken\":")
	}))
	defer srv.Close()
	c := New(srv.URL, "t", time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b, err := c.VolcanicBatch(context.Background(), "", "corr")
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Items) != 1 || !b.Truncated {
		t.Fatalf("%+v", b)
	}
}

func FuzzDecodeBatch(f *testing.F) {
	f.Add([]byte("[" + good1 + "]"))
	f.Add([]byte("[{\"a\":[[[[\"\\\"\"]]]]}"))
	f.Add([]byte("[\xff"))
	f.Fuzz(func(t *testing.T, data []byte) {
		b := DecodeBatch(data, false)
		for _, it := range b.Items {
			if it.Report == nil {
				t.Fatal("item tanpa report")
			}
		}
	})
}
