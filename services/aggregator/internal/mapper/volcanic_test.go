package mapper

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"aggregator/internal/hazard"
)

func validVolcanicReport() map[string]any {
	return map[string]any{
		"report_id":           "vr-001",
		"volcano_id":          "G-001",
		"alert_level":         "Waspada",
		"eruption_count_24h":  float64(2),
		"ash_column_height_m": 850.5,
		"reported_at":         "2026-10-06T02:03:04Z",
	}
}

func TestMapVolcanicReportPreservesUnknownFields(t *testing.T) {
	raw := validVolcanicReport()
	raw["confidence_level"] = 0.87
	raw["observations"] = map[string]any{"visual": "clear", "verified": true}
	raw["sensor_ids"] = []any{"s-1", "s-2"}

	before := time.Now().UTC()
	event, err := MapVolcanicReport(raw)
	if err != nil {
		t.Fatalf("MapVolcanicReport() error = %v", err)
	}
	after := time.Now().UTC()

	wantOccurredAt, _ := time.Parse(time.RFC3339, "2026-10-06T02:03:04Z")
	if event.HazardID != "hz-vr-001" || event.SourceRefID != "vr-001" {
		t.Fatalf("unexpected identifiers: hazard_id=%q source_ref_id=%q", event.HazardID, event.SourceRefID)
	}
	if event.Source != hazard.SourcePVMBG || event.HazardType != hazard.TypeVolcanic {
		t.Fatalf("unexpected source/type: %q/%q", event.Source, event.HazardType)
	}
	if event.Severity != hazard.SeverityWaspada {
		t.Fatalf("severity = %q, want %q", event.Severity, hazard.SeverityWaspada)
	}
	if event.AreaName != "Gunung Merapi" || event.Latitude != -7.5407 || event.Longitude != 110.4457 {
		t.Fatalf("unexpected canonical location: %#v", event)
	}
	if !event.OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("occurred_at = %s, want %s", event.OccurredAt, wantOccurredAt)
	}
	if event.IngestedAt.Before(before) || event.IngestedAt.After(after) {
		t.Fatalf("ingested_at %s is outside mapping interval [%s, %s]", event.IngestedAt, before, after)
	}

	wantAttributes := map[string]any{
		"confidence_level":    0.87,
		"observations":        map[string]any{"visual": "clear", "verified": true},
		"sensor_ids":          []any{"s-1", "s-2"},
		"eruption_count_24h":  2,
		"ash_column_height_m": 850.5,
	}
	if !reflect.DeepEqual(event.Attributes, wantAttributes) {
		t.Fatalf("attributes mismatch\n got: %#v\nwant: %#v", event.Attributes, wantAttributes)
	}
}

func TestMapVolcanicReportRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{
			name: "missing required field",
			mutate: func(raw map[string]any) {
				delete(raw, "report_id")
			},
			wantErr: `missing required field "report_id"`,
		},
		{
			name: "renamed required field rejects entire record",
			mutate: func(raw map[string]any) {
				delete(raw, "report_id")
				raw["observation_id"] = "vr-001"
			},
			wantErr: `missing required field "report_id"`,
		},
		{
			name: "alert level is unknown",
			mutate: func(raw map[string]any) {
				raw["alert_level"] = "Level V"
			},
			wantErr: `field "alert_level" has unknown value "Level V"`,
		},
		{
			name: "eruption count has wrong type",
			mutate: func(raw map[string]any) {
				raw["eruption_count_24h"] = "2"
			},
			wantErr: `field "eruption_count_24h" must be a number`,
		},
		{
			name: "invalid timestamp",
			mutate: func(raw map[string]any) {
				raw["reported_at"] = "yesterday"
			},
			wantErr: "invalid reported_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := validVolcanicReport()
			tt.mutate(raw)

			event, err := MapVolcanicReport(raw)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("MapVolcanicReport() error = %v, want containing %q", err, tt.wantErr)
			}
			if !reflect.DeepEqual(event, hazard.Event{}) {
				t.Fatalf("rejected record returned a partial event: %#v", event)
			}
		})
	}
}

func TestMapVolcanicReportKnownAndUnknownVolcano(t *testing.T) {
	tests := []struct {
		name      string
		volcanoID string
		wantArea  string
		wantLat   float64
		wantLon   float64
	}{
		{name: "known", volcanoID: "G-001", wantArea: "Gunung Merapi", wantLat: -7.5407, wantLon: 110.4457},
		{name: "unknown", volcanoID: "G-999", wantArea: "Unknown Volcano (G-999)", wantLat: 0, wantLon: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := validVolcanicReport()
			raw["volcano_id"] = tt.volcanoID
			event, err := MapVolcanicReport(raw)
			if err != nil {
				t.Fatalf("MapVolcanicReport() error = %v", err)
			}
			if event.AreaName != tt.wantArea || event.Latitude != tt.wantLat || event.Longitude != tt.wantLon {
				t.Fatalf("location = %q (%v,%v), want %q (%v,%v)", event.AreaName, event.Latitude, event.Longitude, tt.wantArea, tt.wantLat, tt.wantLon)
			}
		})
	}
}

func TestMapVolcanicReportRejectsFractionalEruptionCount(t *testing.T) {
	raw := validVolcanicReport()
	raw["eruption_count_24h"] = 3.7

	if _, err := MapVolcanicReport(raw); err == nil || !strings.Contains(err.Error(), "eruption_count_24h") {
		t.Fatalf("MapVolcanicReport() error = %v, want rejection naming eruption_count_24h", err)
	}
}
