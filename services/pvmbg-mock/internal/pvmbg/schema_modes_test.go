package pvmbg

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func newCompatHarness() *harness {
	h := &harness{logs: &bytes.Buffer{}, store: NewStore(20, 4031, fixedClock)}
	srv := NewServer(Config{Token: testToken, DelayMin: 500 * time.Millisecond, DelayMax: 3 * time.Second, TesterCompat: true}, h.store, h.logs)
	srv.sleep = func(time.Duration) {}
	h.handler = srv.Handler()
	return h
}

func TestIncompatibleModesChangeOnlyNewReports(t *testing.T) {
	cases := map[string]func(map[string]any) bool{
		ChangeRename:  func(r map[string]any) bool { _, a := r["alert_level"]; _, b := r["level_status"]; return !a && b },
		ChangeType:    func(r map[string]any) bool { _, ok := r["eruption_count_24h"].(string); return ok },
		ChangeMissing: func(r map[string]any) bool { _, ok := r["volcano_id"]; return !ok },
	}
	for change, check := range cases {
		t.Run(change, func(t *testing.T) {
			h := newHarness()
			rec := h.do(http.MethodPost, "/admin/schema-version", `{"mode":"incompatible","change":"`+change+`"}`, bearer())
			if rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body.String())
			}
			reports := h.reports(t)
			if len(reports) != 21 {
				t.Fatalf("reports = %d", len(reports))
			}
			for i, r := range reports {
				if got := check(r); got != (i == 20) {
					t.Fatalf("report %d: perubahan=%v, hanya report terbaru yang boleh berubah", i, got)
				}
			}
			off := h.do(http.MethodPost, "/admin/schema-version", `{"mode":"incompatible","enabled":false}`, bearer())
			if off.Code != http.StatusOK || h.store.Incompatible() != "" {
				t.Fatalf("mematikan gagal: %d %s", off.Code, off.Body.String())
			}
		})
	}
}

func TestCorruptModeBreaksOnlyTheNextResponses(t *testing.T) {
	for _, kind := range []string{CorruptTruncated, CorruptDeep, CorruptInvalidUTF8, CorruptOversize} {
		t.Run(kind, func(t *testing.T) {
			h := newHarness()
			if rec := h.do(http.MethodPost, "/admin/schema-version", `{"mode":"corrupt","kind":"`+kind+`"}`, bearer()); rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body.String())
			}
			rec := h.do(http.MethodGet, "/volcanic-reports", "", bearer())
			body := rec.Body.Bytes()
			var out []map[string]any
			parseErr := json.Unmarshal(body, &out)
			switch kind {
			case CorruptTruncated:
				if parseErr == nil {
					t.Fatal("respons terpotong seharusnya bukan JSON valid")
				}
			case CorruptInvalidUTF8:
				if utf8.Valid(body) {
					t.Fatal("respons seharusnya memuat UTF-8 tidak valid")
				}
			case CorruptDeep:
				if !strings.Contains(string(body), strings.Repeat("[", 1000)) {
					t.Fatal("respons seharusnya memuat nesting sangat dalam")
				}
			case CorruptOversize:
				if len(body) < 200<<10 {
					t.Fatalf("respons seharusnya berukuran besar, dapat %d", len(body))
				}
			}
			if got := h.reports(t); len(got) != 20 {
				t.Fatalf("respons berikutnya harus normal, dapat %d report", len(got))
			}
		})
	}
}

func TestTesterCompatAcceptsUnauthenticatedAndUnknownBodies(t *testing.T) {
	h := newCompatHarness()
	rec := h.do(http.MethodPost, "/admin/schema-version", `{"version":"v2"}`, nil)
	if rec.Code != http.StatusOK || h.store.Incompatible() != ChangeAll {
		t.Fatalf("%d %s incompatible=%q", rec.Code, rec.Body.String(), h.store.Incompatible())
	}
	if rec := h.do(http.MethodPost, "/admin/schema-version", `{"enabled":false}`, map[string]string{"Authorization": "Bearer salah"}); rec.Code != http.StatusForbidden {
		t.Fatalf("token yang salah tetap harus ditolak, dapat %d", rec.Code)
	}
	if !strings.Contains(h.logs.String(), `"event":"admin_request"`) {
		t.Fatal("permintaan admin harus tercatat agar format dari Tester Agent bisa dipelajari")
	}
}

func TestStrictModeRejectsUnknownMode(t *testing.T) {
	h := newHarness()
	if rec := h.do(http.MethodPost, "/admin/schema-version", `{"mode":"chaos"}`, bearer()); rec.Code != http.StatusBadRequest {
		t.Fatalf("%d", rec.Code)
	}
	if rec := h.do(http.MethodPost, "/admin/schema-version", `{"enabled":true}`, nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("tanpa mode kompatibel, admin wajib bertoken: %d", rec.Code)
	}
}
