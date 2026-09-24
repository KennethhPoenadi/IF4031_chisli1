package api

import (
	"fmt"
	"sort"
	"strings"
)

var (
	summaryFields = []string{"hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"}
	rawFields     = []string{"source_ref_id", "latitude", "longitude", "attributes"}
)

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

type forbiddenError struct { 
	fields []string 
}

func (e *forbiddenError) Error() string {
	return "fields not allowed for this client: " + strings.Join(e.fields, ", ")
}

type unknownError struct {
	 fields []string
}

func (e *unknownError) Error() string {
	return "unknown fields: " + strings.Join(e.fields, ", ")
}

func resolveFields(param string, canRaw bool) ([]string, error) {
	allowed := append([]string{}, summaryFields...)
	if canRaw {
		allowed = append(allowed, rawFields...)
	}
	if strings.TrimSpace(param) == "" {
		return allowed, nil
	}

	var requested, forbidden, unknown []string
	seen := map[string]bool{}
	for _, f := range strings.Split(param, ",") {
		f = strings.TrimSpace(f)
		if f == "" || seen[f] {
			continue
		}
		seen[f] = true
		switch {
		case contains(allowed, f):
			requested = append(requested, f)
		case contains(rawFields, f):
			forbidden = append(forbidden, f)
		default:
			unknown = append(unknown, f)
		}
	}
	if len(forbidden) > 0 {
		sort.Strings(forbidden)
		return nil, &forbiddenError{fields: forbidden}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, &unknownError{fields: unknown}
	}
	if len(requested) == 0 {
		return nil, fmt.Errorf("fields parameter is empty")
	}
	return requested, nil
}

func project(event map[string]any, fields []string) map[string]any {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		if v, ok := event[f]; ok {
			out[f] = v
		}
	}
	return out
}
