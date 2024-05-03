package filter

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

var BasicOperatorMap = map[string]string{
	"$gt":    ">",
	"$gte":   ">=",
	"$lt":    "<",
	"$lte":   "<=",
	"$eq":    "=",
	"$ne":    "!=",
	"$regex": "~*",
}

type Converter struct {
	nestedColumn     string
	nestedExemptions []string
	arrayDriver      func(a any) interface {
		driver.Valuer
		sql.Scanner
	}
}

// NewConverter creates a new Converter with optional nested JSONB field mapping.
//
// Note: When using github.com/lib/pq, the filter.WithArrayDriver should be set to pq.Array.
func NewConverter(options ...Option) *Converter {
	converter := &Converter{}
	for _, option := range options {
		if option != nil {
			option(converter)
		}
	}
	return converter
}

// Convert converts a MongoDB filter query into SQL conditions and values.
//
// startAtParameterIndex is the index to start the parameter numbering at.
// Passing X will make the first indexed parameter $X, the second $X+1, and so on.
func (c *Converter) Convert(query []byte, startAtParameterIndex int) (conditions string, values []any, err error) {
	if startAtParameterIndex < 1 {
		return "", nil, fmt.Errorf("startAtParameterIndex must be greater than 0")
	}

	var mongoFilter map[string]any
	err = json.Unmarshal(query, &mongoFilter)
	if err != nil {
		return "", nil, err
	}

	if len(mongoFilter) == 0 {
		return "TRUE", []any{}, nil
	}

	conditions, values, err = c.convertFilter(mongoFilter, startAtParameterIndex)
	if err != nil {
		return "", nil, err
	}

	return conditions, values, nil
}

func (c *Converter) convertFilter(filter map[string]any, paramIndex int) (string, []any, error) {
	var conditions []string
	var values []any

	if len(filter) == 0 {
		return "", nil, fmt.Errorf("empty objects not allowed")
	}

	keys := []string{}
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := filter[key]

		switch key {
		case "$or", "$and":
			opConditions, ok := anyToSliceMapAny(value)
			if !ok {
				return "", nil, fmt.Errorf("invalid value for $or operator (must be array of objects): %v", value)
			}
			if len(opConditions) == 0 {
				return "", nil, fmt.Errorf("empty arrays not allowed")
			}

			inner := []string{}
			for _, orCondition := range opConditions {
				innerConditions, innerValues, err := c.convertFilter(orCondition, paramIndex)
				if err != nil {
					return "", nil, err
				}
				paramIndex += len(innerValues)
				inner = append(inner, innerConditions)
				values = append(values, innerValues...)
			}
			op := "AND"
			if key == "$or" {
				op = "OR"
			}
			if len(inner) > 1 {
				conditions = append(conditions, "("+strings.Join(inner, " "+op+" ")+")")
			} else {
				conditions = append(conditions, strings.Join(inner, " "+op+" "))
			}
		default:
			if !isValidPostgresIdentifier(key) {
				return "", nil, fmt.Errorf("invalid column name: %s", key)
			}

			switch v := value.(type) {
			case map[string]any:
				if len(v) == 0 {
					return "", nil, fmt.Errorf("empty objects not allowed")
				}

				inner := []string{}
				operators := []string{}
				for operator := range v {
					operators = append(operators, operator)
				}
				sort.Strings(operators)
				for _, operator := range operators {
					switch operator {
					case "$or":
						return "", nil, fmt.Errorf("$or as scalar operator not supported")
					case "$and":
						return "", nil, fmt.Errorf("$and as scalar operator not supported")
					case "$in":
						if !isScalarSlice(v[operator]) {
							return "", nil, fmt.Errorf("invalid value for $in operator (must array of primatives): %v", v[operator])
						}
						inner = append(inner, fmt.Sprintf("(%s = ANY($%d))", c.columnName(key), paramIndex))
						paramIndex++
						if c.arrayDriver != nil {
							v[operator] = c.arrayDriver(v[operator])
						}
						values = append(values, v[operator])
					default:
						value := v[operator]
						op, ok := BasicOperatorMap[operator]
						if !ok {
							return "", nil, fmt.Errorf("unknown operator: %s", operator)
						}
						inner = append(inner, fmt.Sprintf("(%s %s $%d)", c.columnName(key), op, paramIndex))
						paramIndex++
						values = append(values, value)
					}
				}
				innerResult := strings.Join(inner, " AND ")
				if len(inner) > 1 {
					innerResult = "(" + innerResult + ")"
				}
				conditions = append(conditions, innerResult)
			default:
				conditions = append(conditions, fmt.Sprintf("(%s = $%d)", c.columnName(key), paramIndex))
				paramIndex++
				values = append(values, value)
			}
		}
	}

	result := strings.Join(conditions, " AND ")
	if len(conditions) > 1 {
		result = "(" + result + ")"
	}
	return result, values, nil
}

func (c *Converter) columnName(column string) string {
	if c.nestedColumn == "" {
		return fmt.Sprintf("%q", column)
	}
	for _, exemption := range c.nestedExemptions {
		if exemption == column {
			return fmt.Sprintf("%q", column)
		}
	}
	return fmt.Sprintf(`%q->>'%s'`, c.nestedColumn, column)
}
