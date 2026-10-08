package volcano

import "testing"

func TestLookupKnownVolcanoes(t *testing.T) {
	tests := []struct {
		id   string
		name string
		lat  float64
		lon  float64
	}{
		{id: "G-001", name: "Gunung Merapi", lat: -7.5407, lon: 110.4457},
		{id: "G-002", name: "Gunung Kelud", lat: -7.9333, lon: 112.3083},
		{id: "G-003", name: "Gunung Semeru", lat: -8.1080, lon: 112.9220},
		{id: "G-004", name: "Gunung Sinabung", lat: 3.1700, lon: 98.3920},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			ref, ok := Lookup(tt.id)
			if !ok || ref.Name != tt.name || ref.Latitude != tt.lat || ref.Longitude != tt.lon {
				t.Fatalf("Lookup(%q) = %#v, %v", tt.id, ref, ok)
			}
		})
	}
}

func TestLookupUnknownVolcano(t *testing.T) {
	for _, id := range []string{"", "G-999", "g-001"} {
		if ref, ok := Lookup(id); ok || ref != (Reference{}) {
			t.Fatalf("Lookup(%q) = %#v, %v, want the zero value and false", id, ref, ok)
		}
	}
}
