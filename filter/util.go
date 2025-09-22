package filter

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func isNumeric(v any) bool {
	// json.Unmarshal returns float64 for all numbers
	// so we only need to check for float64.
	_, ok := v.(float64)
	return ok
}

func isScalar(v any) bool {
	if v == nil {
		return true
	}

	switch v.(type) {
	case bool, float64, string:
		return true
	default:
		return false
	}
}

func isScalarSlice(v any) bool {
	switch v := v.(type) {
	case []any:
		for _, e := range v {
			if !isScalar(e) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func anyToSliceMapAny(v any) ([]map[string]any, bool) {
	switch v := v.(type) {
	case []any:
		var result []map[string]any
		for _, e := range v {
			switch e := e.(type) {
			case map[string]any:
				result = append(result, e)
			default:
				return nil, false
			}
		}
		return result, true
	default:
		return nil, false
	}
}

func isValidPostgresIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	// The first character needs to be a letter or _
	if (s[0] < 'a' || s[0] > 'z') && (s[0] < 'A' || s[0] > 'Z') && s[0] != '_' {
		return false
	}

	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' {
			continue
		}
		return false
	}

	return true
}

func objectInOrder(b []byte) ([]struct {
	Key   string
	Value any
}, error) {
	dec := json.NewDecoder(bytes.NewReader(b))

	// expect {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("expected object, got %v", tok)
	}

	var result []struct {
		Key   string
		Value any
	}

	for dec.More() {
		// key
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("expected string key, got %v", tok)
		}

		// value
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}

		result = append(result, struct {
			Key   string
			Value any
		}{Key: key, Value: v})
	}

	// consume }
	_, err = dec.Token()
	if err != nil {
		return nil, err
	}

	return result, nil
}
