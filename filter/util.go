package filter

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
