package mapper

import (
	"fmt"
	"time"
)

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	default:
		return 0
	}
}

func parseTime(v any) (time.Time, error) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("expected string timestamp, got %T", v)
	}
	return time.Parse(time.RFC3339, s)
}
