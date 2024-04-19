package filter

import (
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
}

// NewConverter creates a new Converter with optional nested JSONB field mapping.
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
func (c *Converter) Convert(query []byte) (string, []any, error) {
	var mongoFilter map[string]any
	err := json.Unmarshal(query, &mongoFilter)
	if err != nil {
		return "", nil, err
	}

	conditions, values, err := c.convertFilter(mongoFilter, 0)
	if err != nil {
		return "", nil, err
	}

	return conditions, values, nil
}

func (c *Converter) convertFilter(filter map[string]any, paramIndex int) (string, []any, error) {
	var conditions []string
	var values []any

	keys := []string{}
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := filter[key]

		switch key {
		case "$or", "$and":
			orConditions, ok := anyToSliceMapAny(value)
			if !ok {
				return "", nil, fmt.Errorf("invalid value for $or operator (must be array of objects): %v", value)
			}

			inner := []string{}
			for _, orCondition := range orConditions {
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
			switch v := value.(type) {
			case map[string]any:
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
						inner = append(inner, fmt.Sprintf("(%s = ANY(?))", c.columnName(key)))
						if !isScalarSlice(v[operator]) {
							return "", nil, fmt.Errorf("invalid value for $in operator (must array of primatives): %v", v[operator])
						}
						values = append(values, v[operator])
					default:
						value := v[operator]
						op, ok := BasicOperatorMap[operator]
						if !ok {
							return "", nil, fmt.Errorf("unknown operator: %s", operator)
						}
						paramIndex++
						inner = append(inner, fmt.Sprintf("(%s %s $%d)", c.columnName(key), op, paramIndex))
						values = append(values, value)
					}
				}
				innerResult := strings.Join(inner, " AND ")
				if len(inner) > 1 {
					innerResult = "(" + innerResult + ")"
				}
				conditions = append(conditions, innerResult)
			default:
				paramIndex++
				conditions = append(conditions, fmt.Sprintf("(%s = $%d)", c.columnName(key), paramIndex))
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

/*
type Converter struct {
	nestedColumn     string
	nestedExemptions []string
}

func NewConverter(options ...Option) *Converter {
	converter := &Converter{}
	for _, option := range options {
		option(converter)
	}
	return converter
}

func (c *Converter) Convert(filter []byte) (conditions string, values []any, err error) {
	expr, err := c.parse(filter)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse filter: %w", err)
	}
	conditions, values, err = expr.ToPostgresWhereClause()
	if err != nil {
		return "", nil, fmt.Errorf("failed to convert expression to where clause: %w", err)
	}
	return
}

type expression interface {
	ToPostgresWhereClause() (string, []any, error)
}

type compoundExpression struct {
	expressions []expression
	operator    string
}

func (e compoundExpression) ToPostgresWhereClause() (string, []any, error) {
	values := []any{}
	conditions := []string{}
	for _, expr := range e.expressions {
		condition, value, err := expr.ToPostgresWhereClause()
		if err != nil {
			return "", nil, fmt.Errorf("failed to convert expression to where clause: %w", err)
		}
		conditions = append(conditions, condition)
		values = append(values, value...)
	}
	return "(" + strings.Join(conditions, " AND ") + ")", values, nil
}

type scalarExpression struct {
	column   string
	operator string
	value    string
}

func (e scalarExpression) ToPostgresWhereClause() (string, []any, error) {
	return fmt.Sprintf(`"%s" %s ?`, e.column, e.operator), []any{e.value}, nil
}

func (c *Converter) parse(input []byte) (expression, error) {
	raw := map[string]any{}
	err := json.Unmarshal(input, &raw)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal input: %w", err)
	}
	root := compoundExpression{
		expressions: []expression{},
		operator:    "AND",
	}
	for key, value := range raw {
		expr := convertToExpression(key, value, key)
		if expr == nil {
			return nil, fmt.Errorf("failed to convert expression")
		}
		root.expressions = append(root.expressions, expr)
	}

	if root.operator != "AND" {
		return nil, fmt.Errorf("root operator must be AND")
	}
	return root, nil
}

func convertToExpression(key string, value any, currentColumn string) expression {
	switch value := value.(type) {
	case int:
	case int64:
	case float64:
	case string:
		switch key {
		case "$gt":
			return &scalarExpression{
				column:   currentColumn,
				operator: ">",
				value:    value,
			}
		case "$gte":
			return &scalarExpression{
				column:   currentColumn,
				operator: ">=",
				value:    value,
			}
		case "$lt":
			return &scalarExpression{
				column:   currentColumn,
				operator: "<",
				value:    value,
			}
		case "$lte":
			return &scalarExpression{
				column:   currentColumn,
				operator: "<=",
				value:    value,
			}
		case "$eq":
			fallthrough
		default:
			return &scalarExpression{
				column:   currentColumn,
				operator: "=",
				value:    value,
			}
		}
	case map[string]any:
		return convertToExpression(key, value, key)
	}
	return nil
}
*/
