package mapper

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

func requireString(raw map[string]any, key string) (string, error) {
	v, ok := raw[key]
	if !ok {
		return "", fmt.Errorf("missing required field %q", key)
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", fmt.Errorf("field %q must be a non-empty string, got %T", key, v)
	}
	return s, nil
}

func requireFloat(raw map[string]any, key string) (float64, error) {
	v, ok := raw[key]
	if !ok {
		return 0, fmt.Errorf("missing required field %q", key)
	}
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("field %q must be a number, got %T", key, v)
	}
}

func requireInt(raw map[string]any, key string) (int, error) {
	v, ok := raw[key]
	if !ok {
		return 0, fmt.Errorf("missing required field %q", key)
	}
	switch n := v.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || math.Trunc(n) != n {
			return 0, fmt.Errorf("field %q must be an integer, got %v", key, n)
		}

		intLimit := float64(uint64(1) << (strconv.IntSize - 1))
		if n < -intLimit || n >= intLimit {
			return 0, fmt.Errorf("field %q must fit in an int, got %v", key, n)
		}
		return int(n), nil
	case int:
		return n, nil
	default:
		return 0, fmt.Errorf("field %q must be a number, got %T", key, v)
	}
}

func requireBool(raw map[string]any, key string) (bool, error) {
	v, ok := raw[key]
	if !ok {
		return false, fmt.Errorf("missing required field %q", key)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("field %q must be a boolean, got %T", key, v)
	}
	return b, nil
}

func requireEnum(raw map[string]any, key string, allowed map[string]bool) (string, error) {
	s, err := requireString(raw, key)
	if err != nil {
		return "", err
	}
	if !allowed[s] {
		return "", fmt.Errorf("field %q has unknown value %q", key, s)
	}
	return s, nil
}

func requireTime(raw map[string]any, key string) (time.Time, error) {
	s, err := requireString(raw, key)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("field %q must be an RFC3339 timestamp: %w", key, err)
	}
	return t, nil
}
