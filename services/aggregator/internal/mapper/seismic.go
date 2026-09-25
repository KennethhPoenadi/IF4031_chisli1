package mapper

import (
	"fmt"
	"time"

	"aggregator/internal/hazard"
)

var seismicKnownFields = map[string]bool{
	"event_id":          true,
	"magnitude":         true,
	"depth_km":          true,
	"epicenter_lat":     true,
	"epicenter_lon":     true,
	"region_name":       true,
	"occurred_at":       true,
	"potential_tsunami": true,
}

func MapSeismicEvent(raw map[string]any, warning map[string]any) (hazard.Event, error) {
	eventID, _ := raw["event_id"].(string)
	regionName, _ := raw["region_name"].(string)
	magnitude := toFloat(raw["magnitude"])
	epicenterLat := toFloat(raw["epicenter_lat"])
	epicenterLon := toFloat(raw["epicenter_lon"])

	occurredAt, err := parseTime(raw["occurred_at"])
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid occurred_at: %w", err)
	}

	attributes := map[string]any{}
	for key, value := range raw {
		if !seismicKnownFields[key] {
			attributes[key] = value
		}
	}
	attributes["magnitude"] = magnitude
	attributes["depth_km"] = toFloat(raw["depth_km"])
	attributes["potential_tsunami"] = raw["potential_tsunami"]

	severity := severityFromMagnitude(magnitude)
	if warning != nil {
		threatLevel, _ := warning["threat_level"].(string)
		severity = severityFromThreatLevel(threatLevel)
		for key, value := range warning {
			attributes["tsunami_warning_"+key] = value
		}
	}

	return hazard.Event{
		HazardID:    "hz-" + eventID,
		Source:      hazard.SourceBMKG,
		SourceRefID: eventID,
		HazardType:  hazard.TypeSeismic,
		Severity:    severity,
		AreaName:    regionName,
		Latitude:    epicenterLat,
		Longitude:   epicenterLon,
		OccurredAt:  occurredAt,
		IngestedAt:  time.Now().UTC(),
		Attributes:  attributes,
	}, nil
}

func severityFromMagnitude(magnitude float64) hazard.Severity {
	switch {
	case magnitude >= 6.5:
		return hazard.SeveritySiaga
	case magnitude >= 5.0:
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}

func severityFromThreatLevel(threatLevel string) hazard.Severity {
	switch threatLevel {
	case "Awas":
		return hazard.SeverityAwas
	case "Siaga":
		return hazard.SeveritySiaga
	case "Waspada":
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}
