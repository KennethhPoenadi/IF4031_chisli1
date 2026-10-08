package api

import (
	"errors"
	"reflect"
	"testing"
)

func TestResolveFieldsDefaultsDependOnRawAccess(t *testing.T) {
	media, err := resolveFields("", false)
	if err != nil {
		t.Fatalf("resolveFields() error = %v", err)
	}
	for _, field := range rawFields {
		if contains(media, field) {
			t.Fatalf("default fields for a client without raw access include %q: %v", field, media)
		}
	}
	if !reflect.DeepEqual(media, summaryFields) {
		t.Fatalf("media default = %v, want exactly the summary fields %v", media, summaryFields)
	}

	field, err := resolveFields("   ", true)
	if err != nil {
		t.Fatalf("resolveFields() error = %v", err)
	}
	for _, want := range append(append([]string{}, summaryFields...), rawFields...) {
		if !contains(field, want) {
			t.Fatalf("default fields for a client with raw access miss %q: %v", want, field)
		}
	}
}

func TestResolveFieldsRejectsExplicitRawRequestWithoutRawAccess(t *testing.T) {
	tests := []struct {
		name      string
		param     string
		wantNames []string
	}{
		{name: "precise coordinates", param: "latitude", wantNames: []string{"latitude"}},
		{name: "attributes blob", param: "attributes", wantNames: []string{"attributes"}},
		{name: "source reference id", param: "source_ref_id", wantNames: []string{"source_ref_id"}},
		{name: "raw mixed with summary is still rejected", param: "hazard_id,severity,longitude,attributes", wantNames: []string{"attributes", "longitude"}},
		{name: "forbidden wins over unknown", param: "nonsense,latitude", wantNames: []string{"latitude"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := resolveFields(tt.param, false)
			var forbidden *forbiddenError
			if !errors.As(err, &forbidden) {
				t.Fatalf("resolveFields(%q) error = %v, want *forbiddenError", tt.param, err)
			}
			if !reflect.DeepEqual(forbidden.fields, tt.wantNames) {
				t.Fatalf("forbidden fields = %v, want %v", forbidden.fields, tt.wantNames)
			}
			if fields != nil {
				t.Fatalf("rejected request still returned fields: %v", fields)
			}
		})
	}
}

func TestResolveFieldsAllowsRawRequestWithRawAccess(t *testing.T) {
	fields, err := resolveFields("hazard_id,latitude,attributes", true)
	if err != nil {
		t.Fatalf("resolveFields() error = %v", err)
	}
	want := []string{"hazard_id", "latitude", "attributes"}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %v, want %v", fields, want)
	}
}

func TestResolveFieldsHandlesMalformedInput(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		_, err := resolveFields("hazard_id,does_not_exist", true)
		var unknown *unknownError
		if !errors.As(err, &unknown) || !reflect.DeepEqual(unknown.fields, []string{"does_not_exist"}) {
			t.Fatalf("error = %v, want *unknownError for does_not_exist", err)
		}
	})

	t.Run("duplicates and spaces are normalised", func(t *testing.T) {
		fields, err := resolveFields(" severity , severity,hazard_id ", false)
		if err != nil {
			t.Fatalf("resolveFields() error = %v", err)
		}
		if want := []string{"severity", "hazard_id"}; !reflect.DeepEqual(fields, want) {
			t.Fatalf("fields = %v, want %v", fields, want)
		}
	})

	t.Run("only separators", func(t *testing.T) {
		if _, err := resolveFields(",,", false); err == nil {
			t.Fatal("a fields parameter with no field names must be rejected")
		}
	})
}

func TestProjectEmitsOnlyRequestedFields(t *testing.T) {
	event := map[string]any{
		"hazard_id":     "hz-1",
		"source":        "BMKG",
		"severity":      "AWAS",
		"source_ref_id": "bmkg-000001",
		"latitude":      -6.5,
		"longitude":     105.2,
		"attributes":    map[string]any{"magnitude": 7.1},
	}

	media, err := resolveFields("", false)
	if err != nil {
		t.Fatal(err)
	}
	got := project(event, media)
	for _, leaked := range rawFields {
		if _, ok := got[leaked]; ok {
			t.Fatalf("raw field %q leaked into a summary-only projection: %v", leaked, got)
		}
	}
	if got["hazard_id"] != "hz-1" || got["severity"] != "AWAS" || got["source"] != "BMKG" {
		t.Fatalf("summary fields missing from projection: %v", got)
	}

	if got := project(event, []string{"severity", "not_in_event"}); len(got) != 1 || got["severity"] != "AWAS" {
		t.Fatalf("projection must skip fields absent from the event, got %v", got)
	}
}
