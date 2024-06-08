package filter

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

var basicOperatorMap = map[string]string{
	"$gt":    ">",
	"$gte":   ">=",
	"$lt":    "<",
	"$lte":   "<=",
	"$eq":    "=",
	"$ne":    "!=",
	"$regex": "~*",
}

// ReservedColumnName is a reserved column name used internally for nested $elemMatch queries.
// This column name should not be used in the database or any JSONB column.
// You can set this to a different value as long as it's a valid Postgres identifier.
var ReservedColumnName = "__placeholder"

type Converter struct {
	nestedColumn     string
	nestedExemptions []string
	arrayDriver      func(a any) interface {
		driver.Valuer
		sql.Scanner
	}
	emptyCondition string
}

// NewConverter creates a new Converter with optional nested JSONB field mapping.
//
// Note: When using github.com/lib/pq, the filter.WithArrayDriver should be set to pq.Array.
func NewConverter(options ...Option) *Converter {
	converter := &Converter{
		emptyCondition: "FALSE",
	}
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

	if len(query) == 0 {
		return c.emptyCondition, nil, nil
	}

	var mongoFilter map[string]any
	err = json.Unmarshal(query, &mongoFilter)
	if err != nil {
		return "", nil, err
	}

	if len(mongoFilter) == 0 {
		return c.emptyCondition, nil, nil
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
		case "$or", "$and", "$nor":
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
			if key == "$nor" {
				conditions = append(conditions, "NOT ("+strings.Join(inner, " OR ")+")")
			} else {
				op := "AND"
				if key == "$or" {
					op = "OR"
				}
				if len(inner) > 1 {
					conditions = append(conditions, "("+strings.Join(inner, " "+op+" ")+")")
				} else {
					conditions = append(conditions, strings.Join(inner, " "+op+" "))
				}
			}
		case "$not":
			vv, ok := value.(map[string]any)
			if !ok {
				return "", nil, fmt.Errorf("invalid value for $not operator (must be object): %v", value)
			}
			innerConditions, innerValues, err := c.convertFilter(vv, paramIndex)
			if err != nil {
				return "", nil, err
			}
			paramIndex += len(innerValues)
			conditions = append(conditions, "NOT "+innerConditions)
			values = append(values, innerValues...)
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
					case "$not":
						return "", nil, fmt.Errorf("$not as scalar operator not supported")
					case "$in", "$nin":
						if !isScalarSlice(v[operator]) {
							return "", nil, fmt.Errorf("invalid value for $in operator (must array of primatives): %v", v[operator])
						}
						op := ""
						if operator == "$nin" {
							op = "NOT "
						}
						// `column != ANY(...)` does not work, so we need to do `NOT column = ANY(...)` instead.
						inner = append(inner, fmt.Sprintf("(%s%s = ANY($%d))", op, c.columnName(key), paramIndex))
						paramIndex++
						if c.arrayDriver != nil {
							v[operator] = c.arrayDriver(v[operator])
						}
						values = append(values, v[operator])
					case "$exists":
						// $exists only works on jsonb columns, so we need to check if the key is in the JSONB data first.
						isNestedColumn := c.nestedColumn != ""
						for _, exemption := range c.nestedExemptions {
							if exemption == key {
								isNestedColumn = false
								break
							}
						}
						if !isNestedColumn {
							// There is no way in Postgres to check if a column exists on a table.
							return "", nil, fmt.Errorf("$exists operator not supported on non-nested jsonb columns")
						}
						neg := ""
						if v[operator] == false {
							neg = "NOT "
						}
						inner = append(inner, fmt.Sprintf("(%sjsonb_path_match(%s, 'exists($.%s)'))", neg, c.nestedColumn, key))
					case "$elemMatch":
						// $elemMatch needs a different implementation depending on if the column is in JSONB or not.
						isNestedColumn := c.nestedColumn != ""
						for _, exemption := range c.nestedExemptions {
							if exemption == key {
								isNestedColumn = false
								break
							}
						}
						innerConditions, innerValues, err := c.convertFilter(map[string]any{ReservedColumnName: v[operator]}, paramIndex)
						if err != nil {
							return "", nil, err
						}
						paramIndex += len(innerValues)
						if isNestedColumn {
							// This will for example become:
							//
							//   EXISTS (SELECT 1 FROM jsonb_array_elements("meta"->'foo') AS __placeholder WHERE ("__placeholder"::text = $1))
							//
							// We can't use c.columnName here because we need `->` to get the jsonb value instead of `->>` which gets the text value.
							inner = append(inner, fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements(%q->'%s') AS %s WHERE %s)", c.nestedColumn, key, ReservedColumnName, innerConditions))
						} else {
							// This will for example become:
							//
							//   EXISTS (SELECT 1 FROM unnest("foo") AS __placeholder WHERE ("__placeholder"::text = $1))
							//
							inner = append(inner, fmt.Sprintf("EXISTS (SELECT 1 FROM unnest(%s) AS %s WHERE %s)", c.columnName(key), ReservedColumnName, innerConditions))
						}
						values = append(values, innerValues...)
					default:
						value := v[operator]
						op, ok := basicOperatorMap[operator]
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
			case []string, []float64, []bool, []any:
				conditions = append(conditions, fmt.Sprintf("(%s = ANY($%d))", c.columnName(key), paramIndex))
				paramIndex++
				if c.arrayDriver != nil {
					value = c.arrayDriver(value)
				}
				values = append(values, value)
			case nil:
				// Comparing a column to NULL needs a different implementation depending on if the column is in JSONB or not.
				// JSONB columns are NULL even if they don't exist, so we need to check if the column exists first.
				isNestedColumn := c.nestedColumn != ""
				for _, exemption := range c.nestedExemptions {
					if exemption == key {
						isNestedColumn = false
						break
					}
				}
				if isNestedColumn {
					conditions = append(conditions, fmt.Sprintf("(jsonb_path_match(%s, 'exists($.%s)') AND %s IS NULL)", c.nestedColumn, key, c.columnName(key)))
				} else {
					conditions = append(conditions, fmt.Sprintf("(%s IS NULL)", c.columnName(key)))
				}
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
	if column == ReservedColumnName {
		return fmt.Sprintf(`%q::text`, column)
	}
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
