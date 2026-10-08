package mapper

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"aggregator/internal/hazard"
)

func validSeismicEvent() map[string]any {
	return map[string]any{
		"event_id":          "eq-001",
		"magnitude":         5.7,
		"depth_km":          12.5,
		"epicenter_lat":     -7.2,
		"epicenter_lon":     107.6,
		"region_name":       "West Java",
		"occurred_at":       "2026-10-06T01:02:03Z",
		"potential_tsunami": false,
	}
}

func TestMapSeismicEventPreservesUnknownFields(t *testing.T) {
	raw := validSeismicEvent()
	raw["confidence_level"] = 0.91
	raw["provider_metadata"] = map[string]any{"reviewed": true, "station": "BDG"}
	raw["station_codes"] = []any{"BDG", "JKT"}
	warning := map[string]any{
		"warning_id":       "warn-001",
		"related_event_id": "eq-001",
		"threat_level":     "Siaga",
	}

	before := time.Now().UTC()
	event, err := MapSeismicEvent(raw, warning)
	if err != nil {
		t.Fatalf("MapSeismicEvent() error = %v", err)
	}
	after := time.Now().UTC()

	wantOccurredAt, _ := time.Parse(time.RFC3339, "2026-10-06T01:02:03Z")
	if event.HazardID != "hz-eq-001" || event.SourceRefID != "eq-001" {
		t.Fatalf("unexpected identifiers: hazard_id=%q source_ref_id=%q", event.HazardID, event.SourceRefID)
	}
	if event.Source != hazard.SourceBMKG || event.HazardType != hazard.TypeSeismic {
		t.Fatalf("unexpected source/type: %q/%q", event.Source, event.HazardType)
	}
	if event.Severity != hazard.SeveritySiaga {
		t.Fatalf("severity = %q, want %q", event.Severity, hazard.SeveritySiaga)
	}
	if event.AreaName != "West Java" || event.Latitude != -7.2 || event.Longitude != 107.6 {
		t.Fatalf("unexpected canonical location: %#v", event)
	}
	if !event.OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("occurred_at = %s, want %s", event.OccurredAt, wantOccurredAt)
	}
	if event.IngestedAt.Before(before) || event.IngestedAt.After(after) {
		t.Fatalf("ingested_at %s is outside mapping interval [%s, %s]", event.IngestedAt, before, after)
	}

	wantAttributes := map[string]any{
		"confidence_level":                 0.91,
		"provider_metadata":                map[string]any{"reviewed": true, "station": "BDG"},
		"station_codes":                    []any{"BDG", "JKT"},
		"magnitude":                        5.7,
		"depth_km":                         12.5,
		"potential_tsunami":                false,
		"tsunami_warning_warning_id":       "warn-001",
		"tsunami_warning_related_event_id": "eq-001",
		"tsunami_warning_threat_level":     "Siaga",
	}
	if !reflect.DeepEqual(event.Attributes, wantAttributes) {
		t.Fatalf("attributes mismatch\n got: %#v\nwant: %#v", event.Attributes, wantAttributes)
	}
}

func TestMapSeismicEventRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		warning map[string]any
		wantErr string
	}{
		{
			name: "missing required field",
			mutate: func(raw map[string]any) {
				delete(raw, "event_id")
			},
			wantErr: `missing required field "event_id"`,
		},
		{
			name: "renamed required field rejects entire record",
			mutate: func(raw map[string]any) {
				delete(raw, "event_id")
				raw["earthquake_id"] = "eq-001"
			},
			wantErr: `missing required field "event_id"`,
		},
		{
			name: "magnitude has wrong type",
			mutate: func(raw map[string]any) {
				raw["magnitude"] = "5.7"
			},
			wantErr: `field "magnitude" must be a number`,
		},
		{
			name: "invalid timestamp",
			mutate: func(raw map[string]any) {
				raw["occurred_at"] = "not-a-time"
			},
			wantErr: "invalid occurred_at",
		},
		{
			name:    "warning missing threat level",
			mutate:  func(map[string]any) {},
			warning: map[string]any{"warning_id": "warn-001"},
			wantErr: `missing required field "threat_level"`,
		},
		{
			name:    "warning threat level has wrong type",
			mutate:  func(map[string]any) {},
			warning: map[string]any{"threat_level": 3},
			wantErr: `field "threat_level" must be a non-empty string`,
		},
		{
			name:    "warning threat level is unknown",
			mutate:  func(map[string]any) {},
			warning: map[string]any{"threat_level": "Darurat"},
			wantErr: `field "threat_level" has unknown value "Darurat"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := validSeismicEvent()
			tt.mutate(raw)

			event, err := MapSeismicEvent(raw, tt.warning)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("MapSeismicEvent() error = %v, want containing %q", err, tt.wantErr)
			}
			if !reflect.DeepEqual(event, hazard.Event{}) {
				t.Fatalf("rejected record returned a partial event: %#v", event)
			}
		})
	}
}

func TestMapSeismicEventMagnitudeBoundaries(t *testing.T) {
	tests := []struct {
		magnitude float64
		want      hazard.Severity
	}{
		{magnitude: 4.99, want: hazard.SeverityNormal},
		{magnitude: 5.0, want: hazard.SeverityWaspada},
		{magnitude: 6.49, want: hazard.SeverityWaspada},
		{magnitude: 6.5, want: hazard.SeveritySiaga},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%g_%s", tt.magnitude, tt.want), func(t *testing.T) {
			raw := validSeismicEvent()
			raw["magnitude"] = tt.magnitude
			event, err := MapSeismicEvent(raw, nil)
			if err != nil {
				t.Fatalf("MapSeismicEvent() error = %v", err)
			}
			if event.Severity != tt.want {
				t.Fatalf("magnitude %v severity = %q, want %q", tt.magnitude, event.Severity, tt.want)
			}
		})
	}
}

func TestMapSeismicEventWarningOverridesMagnitude(t *testing.T) {
	tests := []struct {
		threatLevel string
		want        hazard.Severity
	}{
		{threatLevel: "Waspada", want: hazard.SeverityWaspada},
		{threatLevel: "Siaga", want: hazard.SeveritySiaga},
		{threatLevel: "Awas", want: hazard.SeverityAwas},
	}

	for _, tt := range tests {
		t.Run(tt.threatLevel, func(t *testing.T) {
			raw := validSeismicEvent()
			raw["magnitude"] = 1.0
			event, err := MapSeismicEvent(raw, map[string]any{"threat_level": tt.threatLevel})
			if err != nil {
				t.Fatalf("MapSeismicEvent() error = %v", err)
			}
			if event.Severity != tt.want {
				t.Fatalf("severity = %q, want %q", event.Severity, tt.want)
			}
			if got := event.Attributes["tsunami_warning_threat_level"]; got != tt.threatLevel {
				t.Fatalf("prefixed warning attribute = %#v, want %q", got, tt.threatLevel)
			}
		})
	}
}
