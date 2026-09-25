package mapper

import (
	"fmt"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/volcano"
)

var volcanicKnownFields = map[string]bool{
	"report_id":           true,
	"volcano_id":          true,
	"alert_level":         true,
	"eruption_count_24h":  true,
	"ash_column_height_m": true,
	"reported_at":         true,
}

func MapVolcanicReport(raw map[string]any) (hazard.Event, error) {
	reportID, _ := raw["report_id"].(string)
	volcanoID, _ := raw["volcano_id"].(string)
	alertLevel, _ := raw["alert_level"].(string)

	occurredAt, err := parseTime(raw["reported_at"])
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid reported_at: %w", err)
	}

	ref, known := volcano.Lookup(volcanoID)
	areaName := ref.Name
	if !known {
		areaName = "Unknown Volcano (" + volcanoID + ")"
	}

	attributes := map[string]any{}
	for key, value := range raw {
		if !volcanicKnownFields[key] {
			attributes[key] = value
		}
	}
	attributes["eruption_count_24h"] = raw["eruption_count_24h"]
	attributes["ash_column_height_m"] = raw["ash_column_height_m"]

	return hazard.Event{
		HazardID:    "hz-" + reportID,
		Source:      hazard.SourcePVMBG,
		SourceRefID: reportID,
		HazardType:  hazard.TypeVolcanic,
		Severity:    severityFromAlertLevel(alertLevel),
		AreaName:    areaName,
		Latitude:    ref.Latitude,
		Longitude:   ref.Longitude,
		OccurredAt:  occurredAt,
		IngestedAt:  time.Now().UTC(),
		Attributes:  attributes,
	}, nil
}

func severityFromAlertLevel(alertLevel string) hazard.Severity {
	switch alertLevel {
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
