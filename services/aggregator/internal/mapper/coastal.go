package mapper

import (
	"fmt"
	"math"
	"time"

	"aggregator/internal/hazard"
)

var coastalKnownFields = map[string]bool{
	"observation_id": true, "station_id": true, "observed_at": true,
	"sea_level_anomaly_m": true, "wave_height_m": true, "quality": true, "critical": true,
}

var coastalStationFields = map[string]bool{"station_id": true, "name": true, "province": true, "latitude": true, "longitude": true}

func MapCoastalEvent(raw, station map[string]any) (hazard.Event, error) {
	if raw == nil || station == nil {
		return hazard.Event{}, fmt.Errorf("coastal observation and station must be objects")
	}
	ref, err := requireString(raw, "observation_id")
	if err != nil {
		return hazard.Event{}, err
	}
	stationID, err := requireString(raw, "station_id")
	if err != nil {
		return hazard.Event{}, err
	}
	resolvedID, err := requireString(station, "station_id")
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid station: %w", err)
	}
	if stationID != resolvedID {
		return hazard.Event{}, fmt.Errorf("observation station_id %q does not match resolved station %q", stationID, resolvedID)
	}
	area, err := requireString(station, "name")
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid station: %w", err)
	}
	province, err := requireString(station, "province")
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid station: %w", err)
	}
	lat, err := requireFloat(station, "latitude")
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid station: %w", err)
	}
	lon, err := requireFloat(station, "longitude")
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid station: %w", err)
	}
	if math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90 || lat > 90 {
		return hazard.Event{}, fmt.Errorf("field %q must be between -90 and 90", "latitude")
	}
	if math.IsNaN(lon) || math.IsInf(lon, 0) || lon < -180 || lon > 180 {
		return hazard.Event{}, fmt.Errorf("field %q must be between -180 and 180", "longitude")
	}
	wave, err := requireFloat(raw, "wave_height_m")
	if err != nil {
		return hazard.Event{}, err
	}
	anomaly, err := requireFloat(raw, "sea_level_anomaly_m")
	if err != nil {
		return hazard.Event{}, err
	}
	if math.IsNaN(wave) || math.IsInf(wave, 0) || wave < 0 {
		return hazard.Event{}, fmt.Errorf("field %q must be a finite non-negative number", "wave_height_m")
	}
	if math.IsNaN(anomaly) || math.IsInf(anomaly, 0) {
		return hazard.Event{}, fmt.Errorf("field %q must be a finite number", "sea_level_anomaly_m")
	}
	quality, err := requireString(raw, "quality")
	if err != nil {
		return hazard.Event{}, err
	}
	critical, err := requireBool(raw, "critical")
	if err != nil {
		return hazard.Event{}, err
	}
	occurredAt, err := requireTime(raw, "observed_at")
	if err != nil {
		return hazard.Event{}, fmt.Errorf("invalid observed_at: %w", err)
	}
	attributes := make(map[string]any, len(raw)+1)
	for key, value := range raw {
		if !coastalKnownFields[key] {
			attributes[key] = value
		}
	}
	for key, value := range station {
		if !coastalStationFields[key] {
			attributes["station_"+key] = value
		}
	}
	attributes["station_id"], attributes["sea_level_anomaly_m"], attributes["wave_height_m"] = stationID, anomaly, wave
	attributes["quality"], attributes["critical"], attributes["province"] = quality, critical, province
	severity := hazard.SeverityNormal
	if critical {
		severity = hazard.SeverityAwas
	}
	return hazard.Event{
		HazardID: "hz-" + ref, Source: hazard.SourceSensorPantai, SourceRefID: ref,
		HazardType: hazard.TypeCoastal, Severity: severity, AreaName: area,
		Latitude: lat, Longitude: lon, OccurredAt: occurredAt, IngestedAt: time.Now().UTC(), Attributes: attributes,
	}, nil
}
