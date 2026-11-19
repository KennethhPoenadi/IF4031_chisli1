package pipeline

import "testing"

func TestLatencySummaryPercentiles(t *testing.T) {
	p := &Pipeline{}
	for i := 1; i <= 100; i++ {
		p.recordLatency(float64(i))
	}
	s := p.LatencySummary()
	if s["samples"].(int) != 100 || s["p95_s"].(float64) != 95 || s["max_s"].(float64) != 100 {
		t.Fatalf("ringkasan tidak sesuai: %v", s)
	}
	for i := 0; i < latWindow+10; i++ {
		p.recordLatency(1)
	}
	if got := p.LatencySummary()["samples"].(int); got != latWindow {
		t.Fatalf("jendela harus dibatasi %d, dapat %d", latWindow, got)
	}
}
