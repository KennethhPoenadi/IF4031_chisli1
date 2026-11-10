package mapper

import (
	"math"
	"testing"
	"time"
)

func validCoastal() (map[string]any, map[string]any) {
	observation := map[string]any{
		"observation_id": "obs-17", "station_id": "station-4", "observed_at": "2026-11-10T08:30:00Z",
		"sea_level_anomaly_m": 0.8, "wave_height_m": 3.2, "quality": "GOOD", "critical": true,
		"vendor_note": "retained",
	}
	station := map[string]any{"station_id": "station-4", "name": "Pangandaran", "province": "Jawa Barat", "latitude": -7.7, "longitude": 108.6}
	return observation, station
}

func TestMapCoastalEvent(t *testing.T) {
	observation, station := validCoastal()
	event, err := MapCoastalEvent(observation, station)
	if err != nil {
		t.Fatal(err)
	}
	if event.HazardID != "hz-obs-17" || string(event.Source) != "SENSOR_PANTAI" || string(event.HazardType) != "COASTAL" || string(event.Severity) != "AWAS" {
		t.Fatalf("event identity = %#v", event)
	}
	if event.SourceRefID != "obs-17" || !event.OccurredAt.Equal(time.Date(2026, 11, 10, 8, 30, 0, 0, time.UTC)) {
		t.Fatalf("source identity and time = %#v", event)
	}
	if event.AreaName != "Pangandaran" || event.Latitude != -7.7 || event.Longitude != 108.6 {
		t.Fatalf("location = %#v", event)
	}
	for key, want := range map[string]any{"vendor_note": "retained", "wave_height_m": 3.2, "sea_level_anomaly_m": 0.8, "quality": "GOOD", "critical": true, "station_id": "station-4"} {
		if event.Attributes[key] != want {
			t.Errorf("attribute %s = %#v; want %#v", key, event.Attributes[key], want)
		}
	}
	if event.IngestedAt.Location() != time.UTC {
		t.Fatalf("ingested_at location = %v", event.IngestedAt.Location())
	}
}

func TestMapCoastalCriticalFalseIsNormal(t *testing.T) {
	observation, station := validCoastal()
	observation["critical"] = false
	event, err := MapCoastalEvent(observation, station)
	if err != nil {
		t.Fatal(err)
	}
	if string(event.Severity) != "NORMAL" {
		t.Fatalf("severity = %q", event.Severity)
	}
}

func TestMapCoastalRejectsInvalidRecords(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any, map[string]any){
		"missing station":        func(_, s map[string]any) { clear(s) },
		"missing id":             func(m, _ map[string]any) { delete(m, "observation_id") },
		"out of range latitude":  func(_, s map[string]any) { s["latitude"] = 91.0 },
		"out of range longitude": func(_, s map[string]any) { s["longitude"] = -181.0 },
		"negative wave":          func(m, _ map[string]any) { m["wave_height_m"] = -0.1 },
		"nonfinite wave":         func(m, _ map[string]any) { m["wave_height_m"] = math.Inf(1) },
		"nonfinite anomaly":      func(m, _ map[string]any) { m["sea_level_anomaly_m"] = math.NaN() },
		"missing quality":        func(m, _ map[string]any) { delete(m, "quality") },
		"missing critical":       func(m, _ map[string]any) { delete(m, "critical") },
		"bad time":               func(m, _ map[string]any) { m["observed_at"] = "yesterday" },
		"station mismatch":       func(m, _ map[string]any) { m["station_id"] = "wrong" },
	} {
		t.Run(name, func(t *testing.T) {
			observation, station := validCoastal()
			mutate(observation, station)
			if _, err := MapCoastalEvent(observation, station); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
