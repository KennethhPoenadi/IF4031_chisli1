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

var validThreatLevels = map[string]bool{"Waspada": true, "Siaga": true, "Awas": true}

func MapSeismicEvent(raw map[string]any, warning map[string]any) (hazard.Event, error) {
	eventID, err := requireString(raw, "event_id")
	if err != nil {
		return hazard.Event{}, err
	}
	regionName, err := requireString(raw, "region_name")
	if err != nil {
		return hazard.Event{}, err
	}
	magnitude, err := requireFloat(raw, "magnitude")
	if err != nil {
		return hazard.Event{}, err
	}
	depthKM, err := requireFloat(raw, "depth_km")
	if err != nil {
		return hazard.Event{}, err
	}
	epicenterLat, err := requireFloat(raw, "epicenter_lat")
	if err != nil {
		return hazard.Event{}, err
	}
	epicenterLon, err := requireFloat(raw, "epicenter_lon")
	if err != nil {
		return hazard.Event{}, err
	}
	potentialTsunami, err := requireBool(raw, "potential_tsunami")
	if err != nil {
		return hazard.Event{}, err
	}
	occurredAt, err := requireTime(raw, "occurred_at")
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
	attributes["depth_km"] = depthKM
	attributes["potential_tsunami"] = potentialTsunami

	severity := severityFromMagnitude(magnitude)
	if warning != nil {
		threatLevel, err := requireEnum(warning, "threat_level", validThreatLevels)
		if err != nil {
			return hazard.Event{}, fmt.Errorf("invalid tsunami warning: %w", err)
		}
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
