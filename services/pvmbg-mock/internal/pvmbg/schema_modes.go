package pvmbg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Perubahan skema tidak kompatibel: rename field, perubahan tipe, field wajib hilang.
const (
	ChangeRename  = "rename"
	ChangeType    = "type"
	ChangeMissing = "missing"
	ChangeAll     = "all"
)

// Jenis respons rusak
const (
	CorruptTruncated   = "truncated"
	CorruptDeep        = "deep"
	CorruptInvalidUTF8 = "invalid_utf8"
	CorruptOversize    = "oversize"
	CorruptAll         = "all"
)

var (
	validChanges = map[string]bool{ChangeRename: true, ChangeType: true, ChangeMissing: true, ChangeAll: true}
	validCorrupt = map[string]bool{CorruptTruncated: true, CorruptDeep: true, CorruptInvalidUTF8: true, CorruptOversize: true, CorruptAll: true}
	changeCycle  = []string{ChangeRename, ChangeType, ChangeMissing}
	corruptCycle = []string{CorruptTruncated, CorruptDeep, CorruptInvalidUTF8, CorruptOversize}
)

type reportJSON struct {
	ReportID         string   `json:"report_id"`
	VolcanoID        string   `json:"volcano_id"`
	AlertLevel       string   `json:"alert_level"`
	EruptionCount24H int      `json:"eruption_count_24h"`
	AshColumnHeightM float64  `json:"ash_column_height_m"`
	ReportedAt       string   `json:"reported_at"`
	ConfidenceLevel  *float64 `json:"confidence_level,omitempty"`
}

func (r VolcanicReport) MarshalJSON() ([]byte, error) {
	base := reportJSON{ReportID: r.ReportID, VolcanoID: r.VolcanoID, AlertLevel: r.AlertLevel,
		EruptionCount24H: r.EruptionCount24H, AshColumnHeightM: r.AshColumnHeightM,
		ReportedAt: r.ReportedAt.UTC().Format(time.RFC3339Nano), ConfidenceLevel: r.ConfidenceLevel}
	if r.Variant == "" {
		return json.Marshal(base)
	}
	m := map[string]any{
		"report_id": base.ReportID, "volcano_id": base.VolcanoID, "alert_level": base.AlertLevel,
		"eruption_count_24h": base.EruptionCount24H, "ash_column_height_m": base.AshColumnHeightM,
		"reported_at": base.ReportedAt,
	}
	if base.ConfidenceLevel != nil {
		m["confidence_level"] = *base.ConfidenceLevel
	}
	switch r.Variant {
	case ChangeRename:
		m["level_status"] = m["alert_level"]
		delete(m, "alert_level")
	case ChangeType:
		m["eruption_count_24h"] = fmt.Sprintf("%d", base.EruptionCount24H)
	case ChangeMissing:
		delete(m, "volcano_id")
	}
	return json.Marshal(m)
}

// merusak satu respons sesuai jenisnya. Report asli tidak berubah, sehingga
// polling berikutnya kembali normal.
func corruptBody(body []byte, kind string) []byte {
	insert := func(elem string) []byte {
		body = bytes.TrimSpace(body)
		if len(body) < 2 || body[0] != '[' {
			return []byte("[" + elem + "]")
		}
		rest := body[1:]
		sep := ","
		if bytes.Equal(bytes.TrimSpace(rest), []byte("]")) {
			sep = ""
		}
		return append([]byte("["+elem+sep), rest...)
	}
	switch kind {
	case CorruptTruncated:
		if len(body) <= 2 {
			return []byte(`[{"report_id":"pvmbg-trunc`)
		}
		cut := len(body) * 6 / 10
		if cut < 2 {
			cut = 2
		}
		return body[:cut]
	case CorruptDeep:
		return insert(`{"report_id":"pvmbg-deep","nested":` + strings.Repeat("[", 5000) + strings.Repeat("]", 5000) + `}`)
	case CorruptInvalidUTF8:
		return insert("{\"report_id\":\"pvmbg-bad-\xff\xfe\",\"alert_level\":\"Siaga\"}")
	case CorruptOversize:
		return insert(`{"report_id":"pvmbg-big","padding":"` + strings.Repeat("x", 200<<10) + `"}`)
	}
	return body
}
