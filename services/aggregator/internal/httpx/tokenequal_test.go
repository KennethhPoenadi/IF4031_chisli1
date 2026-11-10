package httpx

import (
	"strings"
	"testing"
)

func TestTokenEqual(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"abc", "abcd", false}, // panjang berbeda: tetap false, tanpa kebocoran panjang
		{"", "", true},
		{"", "x", false},
		{strings.Repeat("a", 4096), strings.Repeat("a", 4096), true},
	}
	for _, tt := range tests {
		if got := TokenEqual(tt.a, tt.b); got != tt.want {
			t.Fatalf("TokenEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
