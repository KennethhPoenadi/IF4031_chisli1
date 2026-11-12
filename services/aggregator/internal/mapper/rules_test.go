package mapper

import (
	"strings"
	"testing"
)

func TestRulesRepairRenamedAndRetypedFields(t *testing.T) {
	raw := map[string]any{
		"report_id": "pvmbg-9", "volcano_id": "G-001", "level_status": "Siaga",
		"eruption_count_24h": "7", "ash_column_height_m": 1200.0, "reported_at": "2026-11-12T02:40:11Z",
	}
	if _, err := MapVolcanicReport(raw); err == nil {
		t.Fatal("tanpa aturan, field yang berganti nama harus ditolak")
	}
	rules, err := ParseRules("level_status:alert_level", "eruption_count_24h:int")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := MapVolcanicReport(rules.Apply(raw))
	if err != nil {
		t.Fatalf("dengan aturan seharusnya terpetakan: %v", err)
	}
	if ev.Attributes["eruption_count_24h"] != 7 || ev.Severity != "SIAGA" {
		t.Fatalf("hasil salah: %+v", ev)
	}
	if _, ok := raw["alert_level"]; ok {
		t.Fatal("Apply tidak boleh mengubah map asli")
	}
}

func TestParseRulesRejectsBadInput(t *testing.T) {
	for _, tc := range []struct{ a, c string }{{"tanpa_titik_dua", ""}, {"", "x:date"}, {":lama", ""}} {
		if _, err := ParseRules(tc.a, tc.c); err == nil {
			t.Fatalf("ParseRules(%q,%q) seharusnya gagal", tc.a, tc.c)
		}
	}
}

func TestSchemaReasonPrefix(t *testing.T) {
	_, err := MapVolcanicReport(map[string]any{})
	if !strings.HasPrefix(SchemaReason(err), "schema_incompatible: ") {
		t.Fatal(SchemaReason(err))
	}
}
