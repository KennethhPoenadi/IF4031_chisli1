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
	HazardID    string         `bson:"hazard_id" json:"hazard_id"`
	Source      Source         `bson:"source" json:"source"`
	SourceRefID string         `bson:"source_ref_id" json:"source_ref_id"`
	HazardType  Type           `bson:"hazard_type" json:"hazard_type"`
	Severity    Severity       `bson:"severity" json:"severity"`
	AreaName    string         `bson:"area_name" json:"area_name"`
	Latitude    float64        `bson:"latitude" json:"latitude"`
	Longitude   float64        `bson:"longitude" json:"longitude"`
	OccurredAt  time.Time      `bson:"occurred_at" json:"occurred_at"`
	IngestedAt  time.Time      `bson:"ingested_at" json:"ingested_at"`
	Attributes  map[string]any `bson:"attributes" json:"attributes"` //ini map nanti yang bikin bisa handle field baru, handle perbedaan dataa
}
