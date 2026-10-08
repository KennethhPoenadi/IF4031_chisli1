package config

import "testing"

func clearOptional(t *testing.T) {
	t.Helper()
	for _, key := range []string{"RABBITMQ_EXCHANGE", "DASHBOARD_UPDATER_QUEUE", "BINDING_KEY", "DASHBOARD_UPDATER_PREFETCH", "DASHBOARD_UPDATER_HEALTH_PORT"} {
		t.Setenv(key, "")
	}
}

func TestLoadRequiresTheBrokerURL(t *testing.T) {
	clearOptional(t)
	t.Setenv("RABBITMQ_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load() must refuse to start without RABBITMQ_URL; credentials may not have a built-in default")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearOptional(t)
	t.Setenv("RABBITMQ_URL", "amqp://u:p@broker:5672/")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RabbitURL != "amqp://u:p@broker:5672/" || cfg.Exchange != "hazard_events" || cfg.Queue != "dashboard-updater-queue" ||
		cfg.BindingKey != "hazard.#" || cfg.Prefetch != 10 || cfg.HealthPort != "8092" {
		t.Fatalf("defaults = %#v", cfg)
	}
}

func TestLoadOverridesAndInvalidNumbers(t *testing.T) {
	clearOptional(t)
	t.Setenv("RABBITMQ_URL", "amqp://u:p@broker:5672/")
	t.Setenv("RABBITMQ_EXCHANGE", "other")
	t.Setenv("DASHBOARD_UPDATER_QUEUE", "q2")
	t.Setenv("BINDING_KEY", "hazard.seismic.*")
	t.Setenv("DASHBOARD_UPDATER_HEALTH_PORT", "9999")
	t.Setenv("DASHBOARD_UPDATER_PREFETCH", "25")

	cfg, _ := Load()
	if cfg.Exchange != "other" || cfg.Queue != "q2" || cfg.BindingKey != "hazard.seismic.*" || cfg.HealthPort != "9999" || cfg.Prefetch != 25 {
		t.Fatalf("overrides = %#v", cfg)
	}

	for _, bad := range []string{"abc", "-3"} {
		t.Setenv("DASHBOARD_UPDATER_PREFETCH", bad)
		if cfg, _ := Load(); cfg.Prefetch != 10 {
			t.Fatalf("prefetch %q = %d, want the default 10", bad, cfg.Prefetch)
		}
	}
}
