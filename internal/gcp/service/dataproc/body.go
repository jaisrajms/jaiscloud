package dataproc

import "encoding/json"

// bodyString returns body[key] as a string, or "" when absent.
func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

// bodyStringMap returns body[key] as a map[string]string, or nil when absent.
func bodyStringMap(body map[string]any, key string) map[string]string {
	if body == nil {
		return nil
	}
	m, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// bodyStringSlice returns body[key] as a []string, or nil when absent.
func bodyStringSlice(body map[string]any, key string) []string {
	if body == nil {
		return nil
	}
	raw, ok := body[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// bodyInt returns body[key] as an int, or 0 when absent or not a number. JSON
// numbers decode as float64 in this package's map-based bodies.
func bodyInt(body map[string]any, key string) int {
	if body == nil {
		return 0
	}
	switch v := body[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return 0
}

// bodyBool returns body[key] as a bool, or false when absent.
func bodyBool(body map[string]any, key string) bool {
	if body == nil {
		return false
	}
	b, _ := body[key].(bool)
	return b
}

// mustJSONMap unmarshals raw JSON bytes into a map, returning nil on failure.
func mustJSONMap(b []byte) map[string]any {
	if len(b) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}
