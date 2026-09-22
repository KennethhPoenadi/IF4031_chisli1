package hazard

import "time"

type Source string

const (
	SourceBMKG  Source = "BMKG"
	SourcePVMBG Source = "PVMBG"
)

type Type string

const (
	TypeSeismic  Type = "SEISMIC"
	TypeVolcanic Type = "VOLCANIC"
)

type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

type Event struct {
	HazardID    string         `json:"hazard_id"`
	Source      Source         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  Type           `json:"hazard_type"`
	Severity    Severity       `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}
