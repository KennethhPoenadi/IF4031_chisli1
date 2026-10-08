package hazard

import "testing"

func TestSeverityRankIsOrdered(t *testing.T) {
	for i, s := range []Severity{SeverityNormal, SeverityWaspada, SeveritySiaga, SeverityAwas} {
		if s.Rank() != i {
			t.Fatalf("%s rank = %d, want %d", s, s.Rank(), i)
		}
	}
	if Severity("BOGUS").Rank() != -1 || Severity("").Rank() != -1 {
		t.Fatal("an unknown severity must rank below every real one")
	}
}
