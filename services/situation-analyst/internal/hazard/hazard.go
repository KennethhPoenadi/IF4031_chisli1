package hazard

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	CategoryTsunami = "TSUNAMI"

	tsunamiWarningKey = "tsunami_warning_threat_level"
	maxAreaRunes      = 80
)

type Event struct {
	HazardID   string                     `json:"hazard_id"`
	Source     string                     `json:"source"`
	HazardType string                     `json:"hazard_type"`
	Severity   string                     `json:"severity"`
	AreaName   string                     `json:"area_name"`
	OccurredAt time.Time                  `json:"occurred_at"`
	IngestedAt time.Time                  `json:"ingested_at"`
	Attributes map[string]json.RawMessage `json:"attributes"`
}

func (e Event) Category() string {
	if strings.EqualFold(e.HazardType, "SEISMIC") {
		if _, ok := e.Attributes[tsunamiWarningKey]; ok {
			return CategoryTsunami
		}
	}
	return strings.ToUpper(strings.TrimSpace(e.HazardType))
}

func (e Event) Area() string {
	var b strings.Builder
	n := 0
	for _, r := range strings.TrimSpace(e.AreaName) {
		if r < 0x20 || r == 0x7f {
			continue
		}
		if n >= maxAreaRunes {
			break
		}
		b.WriteRune(r)
		n++
	}
	if b.Len() == 0 {
		return "(tanpa nama)"
	}
	return b.String()
}

func (e Event) SeverityName() string { return strings.ToUpper(strings.TrimSpace(e.Severity)) }
