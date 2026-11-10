package hazard

import "time"

type Source string

const (
	SourceBMKG         Source = "BMKG"
	SourcePVMBG        Source = "PVMBG"
	SourceSatelit      Source = "SATELIT"
	SourceSensorPantai Source = "SENSOR_PANTAI"
)

type Type string

const (
	TypeSeismic  Type = "SEISMIC"
	TypeVolcanic Type = "VOLCANIC"
	TypeWildfire Type = "WILDFIRE"
	TypeCoastal  Type = "COASTAL"
)

var (
	ValidTypes   = []Type{TypeSeismic, TypeVolcanic, TypeWildfire, TypeCoastal}
	ValidSources = []Source{SourceBMKG, SourcePVMBG, SourceSatelit, SourceSensorPantai}
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

// Kategori dashboard analitik. Lima kategori yang saling lepas: tsunami dipisah dari gempa agar total
// kategori sama dengan jumlah event.
const (
	CategorySeismic  = string(TypeSeismic)
	CategoryTsunami  = "TSUNAMI"
	CategoryVolcanic = string(TypeVolcanic)
	CategoryWildfire = string(TypeWildfire)
	CategoryCoastal  = string(TypeCoastal)
)

// TsunamiWarningKey adalah atribut yang dibawa event SEISMIC bila BMKG menerbitkan peringatan tsunami.
const TsunamiWarningKey = "tsunami_warning_threat_level"

// Category menentukan kategori dashboard sebuah event. Event SEISMIC yang membawa peringatan tsunami dihitung
// sebagai TSUNAMI, bukan SEISMIC.
func (e Event) Category() string {
	if e.HazardType == TypeSeismic {
		if _, ok := e.Attributes[TsunamiWarningKey]; ok {
			return CategoryTsunami
		}
	}
	return string(e.HazardType)
}
