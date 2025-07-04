package filter

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var numericOperatorMap = map[string]string{
	"$gt":  ">",
	"$gte": ">=",
	"$lt":  "<",
	"$lte": "<=",
}

var textOperatorMap = map[string]string{
	"$eq":    "=",
	"$ne":    "!=",
	"$regex": "~*",
}

// defaultPlaceholderName is the default placeholder name used in the generated SQL query.
// This name should not be used in the database or any JSONB column. It can be changed using
// the WithPlaceholderName option.
const defaultPlaceholderName = "__filter_placeholder"

// Converter converts MongoDB filter queries to SQL conditions and values. Use [filter.NewConverter] to create a new instance.
type Converter struct {
	allowAllColumns   bool
	allowedColumns    []string
	disallowedColumns []string
	nestedColumn      string
	nestedExemptions  []string
	arrayDriver       func(a any) interface {
		driver.Valuer
		sql.Scanner
	}
	emptyCondition  string
	placeholderName string

	once sync.Once
}

// NewConverter creates a new [Converter] with optional nested JSONB field mapping.
//
// Note: When using https://github.com/lib/pq, the [filter.WithArrayDriver] should be set to pq.Array.
func NewConverter(options ...Option) (*Converter, error) {
	converter := &Converter{
		// don't set defaults, use the once.Do in #Convert()
	}
	seenAccessOption := false
	for _, option := range options {
		if option.f != nil {
			option.f(converter)
		}
		if option.isAccessOption {
			seenAccessOption = true
		}
	}
	if !seenAccessOption {
		return nil, ErrNoAccessOption
	}
	return converter, nil
}

// Convert converts a MongoDB filter query into SQL conditions and values.
//
// startAtParameterIndex is the index to start the parameter numbering at.
// Passing X will make the first indexed parameter $X, the second $X+1, and so on.
func (c *Converter) Convert(query []byte, startAtParameterIndex int) (conditions string, values []any, err error) {
	c.once.Do(func() {
		if c.emptyCondition == "" {
			c.emptyCondition = "FALSE"
		}
		if c.placeholderName == "" {
			c.placeholderName = defaultPlaceholderName
		}
	})

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
			// Just putting a NOT around the condition is not enough, a non existing jsonb field will for example
			// make the whole inner condition NULL. And NOT NULL is still a falsy value, so we need to check for NULL explicitly.
			conditions = append(conditions, fmt.Sprintf("(NOT COALESCE(%s, FALSE))", innerConditions))
			values = append(values, innerValues...)
		default:
			if !isValidPostgresIdentifier(key) {
				return "", nil, fmt.Errorf("invalid column name: %s", key)
			}
			if !c.isColumnAllowed(key) {
				return "", nil, ColumnNotAllowedError{Column: key}
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
						neg := ""
						if operator == "$nin" {
							// `column != ANY(...)` does not work, so we need to do `NOT column = ANY(...)` instead.
							neg = "NOT "
						}
						inner = append(inner, fmt.Sprintf("(%s%s = ANY($%d))", neg, c.columnName(key), paramIndex))
						paramIndex++
						if c.arrayDriver != nil {
							v[operator] = c.arrayDriver(v[operator])
						}
						values = append(values, v[operator])
					case "$exists":
						// $exists only works on jsonb columns, so we need to check if the key is in the JSONB data first.
						if !c.isNestedColumn(key) {
							// There is no way in Postgres to check if a column exists on a table.
							return "", nil, fmt.Errorf("$exists operator not supported on non-nested jsonb columns")
						}
						neg := ""
						if v[operator] == false {
							neg = "NOT "
						}
						inner = append(inner, fmt.Sprintf("(%sjsonb_path_match(%s, 'exists($.%s)'))", neg, c.nestedColumn, key))
					case "$elemMatch":
						innerConditions, innerValues, err := c.convertFilter(map[string]any{c.placeholderName: v[operator]}, paramIndex)
						if err != nil {
							return "", nil, err
						}
						paramIndex += len(innerValues)

						// $elemMatch needs a different implementation depending on if the column is in JSONB or not.
						if c.isNestedColumn(key) {
							// This will for example become:
							//
							//   EXISTS (SELECT 1 FROM jsonb_array_elements("meta"->'foo') AS __filter_placeholder WHERE ("__filter_placeholder"::text = $1))
							//
							// We can't use c.columnName here because we need `->` to get the jsonb value instead of `->>` which gets the text value.
							inner = append(inner, fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements(%q->'%s') AS %s WHERE %s)", c.nestedColumn, key, c.placeholderName, innerConditions))
						} else {
							// This will for example become:
							//
							//   EXISTS (SELECT 1 FROM unnest("foo") AS __filter_placeholder WHERE ("__filter_placeholder"::text = $1))
							//
							inner = append(inner, fmt.Sprintf("EXISTS (SELECT 1 FROM unnest(%s) AS %s WHERE %s)", c.columnName(key), c.placeholderName, innerConditions))
						}
						values = append(values, innerValues...)
					case "$field":
						vv, ok := v[operator].(string)
						if !ok {
							return "", nil, fmt.Errorf("invalid value for $field operator (must be string): %v", v[operator])
						}

						inner = append(inner, fmt.Sprintf("(%s = %s)", c.columnName(key), c.columnName(vv)))
					default:
						value := v[operator]
						isNumericOperator := false
						op, ok := textOperatorMap[operator]
						if !ok {
							op, ok = numericOperatorMap[operator]
							if !ok {
								return "", nil, fmt.Errorf("unknown operator: %s", operator)
							}
							isNumericOperator = true
						}

						// If the value is a map with a $field key, we need to compare the column to another column.
						if vv, ok := value.(map[string]any); ok {
							field, ok := vv["$field"].(string)
							if !ok || len(vv) > 1 {
								return "", nil, fmt.Errorf("invalid value for %s operator (must be object with $field key only): %v", operator, value)
							}

							left := c.columnName(key)
							right := c.columnName(field)

							if isNumericOperator {
								if c.isNestedColumn(key) {
									left = fmt.Sprintf("(%s)::numeric", left)
								}
								if c.isNestedColumn(field) {
									right = fmt.Sprintf("(%s)::numeric", right)
								}
							}

							inner = append(inner, fmt.Sprintf("(%s %s %s)", left, op, right))
						} else {
							// Prevent cryptic errors like:
							// 	 unexpected error: sql: converting argument $1 type: unsupported type []interface {}, a slice of interface
							if !isScalar(value) {
								return "", nil, fmt.Errorf("invalid comparison value (must be a primitive): %v", value)
							}

							// If we aren't comparing columns, and the field is a numeric scalar, we also see = ($eq) and != ($ne) as numeric operators.
							// This way we can use ::numeric on jsonb values to prevent getting postgres errors like:
							//   ERROR:  operator does not exist: text = numeric
							if isNumeric(value) && !isNumericOperator {
								if op == "=" || op == "!=" {
									isNumericOperator = true
								}
							}

							if isNumericOperator && isNumeric(value) && c.isNestedColumn(key) {
								inner = append(inner, fmt.Sprintf("((%s)::numeric %s $%d)", c.columnName(key), op, paramIndex))
							} else {
								inner = append(inner, fmt.Sprintf("(%s %s $%d)", c.columnName(key), op, paramIndex))
							}
							paramIndex++
							values = append(values, value)
						}
					}
				}
				innerResult := strings.Join(inner, " AND ")
				if len(inner) > 1 {
					innerResult = "(" + innerResult + ")"
				}
				conditions = append(conditions, innerResult)
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
				// Prevent cryptic errors like:
				// 	 unexpected error: sql: converting argument $1 type: unsupported type []interface {}, a slice of interface
				if !isScalar(value) {
					return "", nil, fmt.Errorf("invalid comparison value (must be a primitive): %v", value)
				}
				if isNumeric(value) && c.isNestedColumn(key) {
					// If the value is numeric and the column is a nested JSONB column, we need to cast the column to numeric.
					conditions = append(conditions, fmt.Sprintf("((%s)::numeric = $%d)", c.columnName(key), paramIndex))
				} else {
					conditions = append(conditions, fmt.Sprintf("(%s = $%d)", c.columnName(key), paramIndex))
				}
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
	if column == c.placeholderName {
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

func (c *Converter) isColumnAllowed(column string) bool {
	for _, disallowed := range c.disallowedColumns {
		if disallowed == column {
			return false
		}
	}
	if c.allowAllColumns {
		return true
	}
	if c.nestedColumn != "" {
		return true
	}
	for _, allowed := range c.allowedColumns {
		if allowed == column {
			return true
		}
	}
	return false
}

func (c *Converter) isNestedColumn(column string) bool {
	if c.nestedColumn == "" {
		return false
	}
	for _, exemption := range c.nestedExemptions {
		if exemption == column {
			return false
		}
	}
	return true
}
