package volcano

type Reference struct {
	Name      string
	Latitude  float64
	Longitude float64
}

var referenceTable = map[string]Reference{
	"G-001": {Name: "Gunung Merapi", Latitude: -7.5407, Longitude: 110.4457},
	"G-002": {Name: "Gunung Kelud", Latitude: -7.9333, Longitude: 112.3083},
	"G-003": {Name: "Gunung Semeru", Latitude: -8.1080, Longitude: 112.9220},
	"G-004": {Name: "Gunung Sinabung", Latitude: 3.1700, Longitude: 98.3920},
}

func Lookup(volcanoID string) (Reference, bool) {
	ref, ok := referenceTable[volcanoID]
	return ref, ok
}
