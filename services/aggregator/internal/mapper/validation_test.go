package mapper

import (
	"strings"
	"testing"

	"aggregator/internal/hazard"
)

func TestRequireHelpersAcceptAndRejectTypes(t *testing.T) {
	t.Run("float accepts float64 and int", func(t *testing.T) {
		for _, v := range []any{2.5, 2} {
			got, err := requireFloat(map[string]any{"k": v}, "k")
			if err != nil || got == 0 {
				t.Fatalf("requireFloat(%v) = %v, %v", v, got, err)
			}
		}
	})
	t.Run("int accepts float64 and int", func(t *testing.T) {
		for _, v := range []any{3.0, 3} {
			got, err := requireInt(map[string]any{"k": v}, "k")
			if err != nil || got != 3 {
				t.Fatalf("requireInt(%v) = %v, %v", v, got, err)
			}
		}
	})
	t.Run("bool", func(t *testing.T) {
		if got, err := requireBool(map[string]any{"k": true}, "k"); err != nil || !got {
			t.Fatalf("requireBool(true) = %v, %v", got, err)
		}
	})

	wrong := map[string]any{"k": "text"}
	missing := map[string]any{}
	checks := map[string]func(map[string]any) error{
		"float": func(m map[string]any) error { _, err := requireFloat(m, "k"); return err },
		"int":   func(m map[string]any) error { _, err := requireInt(m, "k"); return err },
		"bool":  func(m map[string]any) error { _, err := requireBool(m, "k"); return err },
		"time":  func(m map[string]any) error { _, err := requireTime(m, "k"); return err },
		"enum":  func(m map[string]any) error { _, err := requireEnum(m, "k", map[string]bool{"a": true}); return err },
	}
	for name, check := range checks {
		t.Run(name+" rejects a missing field", func(t *testing.T) {
			if err := check(missing); err == nil || !strings.Contains(err.Error(), "missing required field") {
				t.Fatalf("error = %v", err)
			}
		})
		t.Run(name+" rejects the wrong type or value", func(t *testing.T) {
			if err := check(wrong); err == nil {
				t.Fatal("expected an error for a string value")
			}
		})
	}

	t.Run("string rejects an empty value", func(t *testing.T) {
		if _, err := requireString(map[string]any{"k": ""}, "k"); err == nil {
			t.Fatal("empty strings are not valid identifiers")
		}
	})
	t.Run("time rejects a non-RFC3339 string", func(t *testing.T) {
		if _, err := requireTime(map[string]any{"k": "08/10/2026"}, "k"); err == nil {
			t.Fatal("expected a parse error")
		}
	})
}

func TestMapSeismicEventRejectsEachMissingRequiredField(t *testing.T) {
	for key := range seismicKnownFields {
		t.Run(key, func(t *testing.T) {
			raw := validSeismicEvent()
			delete(raw, key)
			if _, err := MapSeismicEvent(raw, nil); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("error = %v, want it to name %q", err, key)
			}
		})
	}
}

func TestMapSeismicEventRejectsEachWrongType(t *testing.T) {
	for _, key := range []string{"region_name", "depth_km", "epicenter_lat", "epicenter_lon", "potential_tsunami"} {
		t.Run(key, func(t *testing.T) {
			raw := validSeismicEvent()
			raw[key] = []string{"not", "valid"}
			if _, err := MapSeismicEvent(raw, nil); err == nil {
				t.Fatalf("a wrongly typed %s must reject the record", key)
			}
		})
	}
}

func TestSeverityFromThreatLevelAndAlertLevel(t *testing.T) {
	threat := map[string]hazard.Severity{
		"Waspada": hazard.SeverityWaspada,
		"Siaga":   hazard.SeveritySiaga,
		"Awas":    hazard.SeverityAwas,
		"Normal":  hazard.SeverityNormal,
		"":        hazard.SeverityNormal,
	}
	for in, want := range threat {
		if got := severityFromThreatLevel(in); got != want {
			t.Errorf("severityFromThreatLevel(%q) = %q, want %q", in, got, want)
		}
	}

	alert := map[string]hazard.Severity{
		"Normal":  hazard.SeverityNormal,
		"Waspada": hazard.SeverityWaspada,
		"Siaga":   hazard.SeveritySiaga,
		"Awas":    hazard.SeverityAwas,
		"":        hazard.SeverityNormal,
	}
	for in, want := range alert {
		if got := severityFromAlertLevel(in); got != want {
			t.Errorf("severityFromAlertLevel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMapVolcanicReportMapsEveryAlertLevel(t *testing.T) {
	levels := map[string]hazard.Severity{
		"Normal":  hazard.SeverityNormal,
		"Waspada": hazard.SeverityWaspada,
		"Siaga":   hazard.SeveritySiaga,
		"Awas":    hazard.SeverityAwas,
	}
	for level, want := range levels {
		t.Run(level, func(t *testing.T) {
			raw := validVolcanicReportForValidation()
			raw["alert_level"] = level
			event, err := MapVolcanicReport(raw)
			if err != nil || event.Severity != want {
				t.Fatalf("severity = %q, err = %v, want %q", event.Severity, err, want)
			}
		})
	}
}

func TestMapVolcanicReportRejectsEachMissingRequiredField(t *testing.T) {
	for key := range volcanicKnownFields {
		t.Run(key, func(t *testing.T) {
			raw := validVolcanicReportForValidation()
			delete(raw, key)
			if _, err := MapVolcanicReport(raw); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("error = %v, want it to name %q", err, key)
			}
		})
	}
}

func TestMapVolcanicReportRejectsEachWrongType(t *testing.T) {
	for _, key := range []string{"volcano_id", "eruption_count_24h", "ash_column_height_m", "reported_at"} {
		t.Run(key, func(t *testing.T) {
			raw := validVolcanicReportForValidation()
			raw[key] = []string{"not", "valid"}
			if _, err := MapVolcanicReport(raw); err == nil {
				t.Fatalf("a wrongly typed %s must reject the record", key)
			}
		})
	}

	t.Run("reported_at that is not a timestamp", func(t *testing.T) {
		raw := validVolcanicReportForValidation()
		raw["reported_at"] = "yesterday"
		if _, err := MapVolcanicReport(raw); err == nil || !strings.Contains(err.Error(), "invalid reported_at") {
			t.Fatalf("error = %v", err)
		}
	})
}

func validVolcanicReportForValidation() map[string]any {
	return map[string]any{
		"report_id":           "pvmbg-1",
		"volcano_id":          "G-001",
		"alert_level":         "Siaga",
		"eruption_count_24h":  2.0,
		"ash_column_height_m": 1500.0,
		"reported_at":         "2026-10-08T01:02:03Z",
	}
}
