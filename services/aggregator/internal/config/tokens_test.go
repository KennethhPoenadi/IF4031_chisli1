package config

import (
	"strings"
	"testing"
)

func TestWeakTokensWarnsAboutShortOrSharedTokens(t *testing.T) {
	long := strings.Repeat("a", MinTokenLength)
	if got := (Config{AuditToken: long, DashboardToken: strings.Repeat("b", MinTokenLength)}).WeakTokens(); len(got) != 0 {
		t.Fatalf("token kuat dan terpisah tidak boleh diperingatkan: %v", got)
	}
	if got := (Config{}).WeakTokens(); len(got) != 0 {
		t.Fatalf("token kosong ditangani terpisah, bukan sebagai lemah: %v", got)
	}
	if got := (Config{AuditToken: "pendek"}).WeakTokens(); len(got) != 1 || !strings.Contains(got[0], "AUDIT_TOKEN") {
		t.Fatalf("token pendek harus diperingatkan: %v", got)
	}
	got := (Config{AuditToken: long, DashboardToken: long}).WeakTokens()
	if len(got) != 1 || !strings.Contains(got[0], "sama") {
		t.Fatalf("token yang sama harus diperingatkan: %v", got)
	}
}
