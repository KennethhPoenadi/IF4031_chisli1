package config

import (
	"strings"
	"testing"
	"time"
)

func setBase(t *testing.T) {
	t.Helper()
	t.Setenv("ANALYST_API_KEY", "kunci-internal")
	t.Setenv("RABBITMQ_URL", "amqp://u:p@rabbitmq:5672/")
}

func TestLoadDefaultsAndOptionalLLM(t *testing.T) {
	setBase(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != "8094" || c.Queue != "situation-analyst-queue" || c.WindowMinutes != 15 ||
		c.SummaryInterval != 3*time.Minute || c.LLMTimeout != 20*time.Second || c.LLMMaxPerHour != 20 || c.QueueMaxLength != 2000 {
		t.Fatalf("bawaan = %+v", c)
	}
	if c.LLMEnabled() {
		t.Fatal("tanpa konfigurasi LLM, komponen harus tetap berjalan dengan LLM nonaktif")
	}
}

func TestLoadRequiresInternalKeyAndBroker(t *testing.T) {
	t.Setenv("ANALYST_API_KEY", "")
	t.Setenv("RABBITMQ_URL", "amqp://x")
	if _, err := Load(); err == nil {
		t.Fatal("tanpa ANALYST_API_KEY harus gagal")
	}
	t.Setenv("ANALYST_API_KEY", "k")
	t.Setenv("RABBITMQ_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("tanpa RABBITMQ_URL harus gagal")
	}
}

func TestLoadRejectsBadNumbers(t *testing.T) {
	setBase(t)
	for _, kv := range [][2]string{{"ANALYST_WINDOW_MINUTES", "0"}, {"LLM_TIMEOUT_SECONDS", "abc"}, {"ANALYST_PREFETCH", "-1"}, {"SITUATION_ANALYST_PORT", "99999"}} {
		t.Setenv(kv[0], kv[1])
		if _, err := Load(); err == nil {
			t.Fatalf("%s=%s harus ditolak", kv[0], kv[1])
		}
		t.Setenv(kv[0], "")
	}
}

func TestLLMNeedsAllThreeSettings(t *testing.T) {
	setBase(t)
	t.Setenv("LLM_BASE_URL", "https://api.orcarouter.ai/v1")
	t.Setenv("LLM_MODEL", "deepseek/deepseek-v4-flash-free")
	c, _ := Load()
	if c.LLMEnabled() {
		t.Fatal("tanpa key, LLM harus nonaktif")
	}
	t.Setenv("LLM_API_KEY", "sk-rahasia")
	c, _ = Load()
	if !c.LLMEnabled() || c.LLMHost() != "api.orcarouter.ai" {
		t.Fatalf("enabled=%v host=%q", c.LLMEnabled(), c.LLMHost())
	}
}

func TestStringNeverContainsSecrets(t *testing.T) {
	setBase(t)
	t.Setenv("LLM_BASE_URL", "https://api.orcarouter.ai/v1")
	t.Setenv("LLM_MODEL", "m")
	t.Setenv("LLM_API_KEY", "sk-RAHASIA-123")
	c, _ := Load()
	for _, s := range []string{c.String(), strings.ToLower(c.String())} {
		if strings.Contains(s, "RAHASIA") || strings.Contains(s, "rahasia") || strings.Contains(s, "kunci-internal") || strings.Contains(s, "amqp://u:p") {
			t.Fatalf("String() membocorkan rahasia: %s", s)
		}
	}
}
