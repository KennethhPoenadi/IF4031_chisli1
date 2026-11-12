package llm_test

import (
	"context"
	"os"
	"testing"
	"time"

	"situation-analyst/internal/anomaly"
	"situation-analyst/internal/llm"
	"situation-analyst/internal/summary"
	"situation-analyst/internal/window"
)

func TestLiveProviderRoundTrip(t *testing.T) {
	if os.Getenv("LLM_LIVE") != "1" {
		t.Skip("LLM_LIVE tidak diisi")
	}
	cfg := llm.Config{BaseURL: os.Getenv("LLM_BASE_URL"), Model: os.Getenv("LLM_MODEL"), APIKey: os.Getenv("LLM_API_KEY"), Timeout: 30 * time.Second}
	cl, err := llm.New(cfg, nil, nil)
	if err != nil {
		t.Fatalf("konfigurasi: %v", err)
	}
	if !cl.Enabled() {
		t.Skip("LLM_BASE_URL, LLM_MODEL, dan LLM_API_KEY belum lengkap")
	}
	t.Logf("penyedia=%s model=%s", cfg.BaseURL, cfg.Model)

	snap := window.Snapshot{WindowMinutes: 15, Total: 40,
		ByCategory: map[string]int{"WILDFIRE": 30, "SEISMIC": 8, "TSUNAMI": 2},
		BySeverity: map[string]int{"AWAS": 5, "SIAGA": 9, "WASPADA": 12, "NORMAL": 14},
		TopAreas:   []window.AreaCount{{Area: "Riau", Count: 18}, {Area: "Jambi", Count: 9}}}
	an := []anomaly.Anomaly{{Key: summary.AreaKey("WILDFIRE", "Riau"), Count: 17, Expected: 2.4, PValue: 1e-9}}
	system, user := summary.Prompt(snap, an)

	start := time.Now()
	text, outcome, err := cl.Summarize(context.Background(), system, user)
	t.Logf("hasil=%s durasi=%v", outcome, time.Since(start).Round(time.Millisecond))
	if err != nil {
		t.Fatalf("panggilan gagal: outcome=%s err=%v", outcome, err)
	}
	t.Logf("jawaban model: %q", text)

	valid, verr := summary.ValidateOutput(text, summary.AllowedNumbers(snap, an))
	if verr != nil {
		t.Logf("PERHATIAN: jawaban asli ditolak validasi (aplikasi akan memakai templat): %v", verr)
		return
	}
	t.Logf("lolos validasi: %q", valid)
}
