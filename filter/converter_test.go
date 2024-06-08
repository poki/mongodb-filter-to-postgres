package filter_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func TestConverter_Convert(t *testing.T) {
	tests := []struct {
		name       string
		option     filter.Option
		input      string
		conditions string
		values     []any
		err        error
	}{
		{
			"flat single value",
			nil,
			`{"name": "John"}`,
			`("name" = $1)`,
			[]any{"John"},
			nil,
		},
		{
			"flat multi value",
			nil,
			`{"age": 30, "name": "John"}`,
			`(("age" = $1) AND ("name" = $2))`,
			[]any{float64(30), "John"},
			nil,
		},
		{
			"operator single value",
			nil,
			`{"players": {"$gt": 0}}`,
			`("players" > $1)`,
			[]any{float64(0)},
			nil,
		},
		{
			"operator flat multi value",
			nil,
			`{"age": {"$gte": 18}, "name": "John"}`,
			`(("age" >= $1) AND ("name" = $2))`,
			[]any{float64(18), "John"},
			nil,
		},
		{
			"nested jsonb single value",
			filter.WithNestedJSONB("meta"),
			`{"name": "John"}`,
			`("meta"->>'name' = $1)`,
			[]any{"John"},
			nil,
		},
		{
			"nested jsonb multi value",
			filter.WithNestedJSONB("meta", "created_at", "updated_at"),
			`{"created_at": {"$gte": "2020-01-01T00:00:00Z"}, "name": "John", "role": "admin"}`,
			`(("created_at" >= $1) AND ("meta"->>'name' = $2) AND ("meta"->>'role' = $3))`,
			[]any{"2020-01-01T00:00:00Z", "John", "admin"},
			nil,
		},
		{
			"fields are order alphabetically",
			nil,
			`{"b": 1, "c": 2, "a": 3}`,
			`(("a" = $1) AND ("b" = $2) AND ("c" = $3))`,
			[]any{float64(3), float64(1), float64(2)},
			nil,
		},
		{
			"in-array operator simple",
			nil,
			`{"status": {"$in": ["NEW", "OPEN"]}}`,
			`("status" = ANY($1))`,
			[]any{[]any{"NEW", "OPEN"}},
			nil,
		},
		{
			"in-array operator invalid value",
			nil,
			`{"status": {"$in": [{"hacker": 1}, "OPEN"]}}`,
			``,
			nil,
			fmt.Errorf("invalid value for $in operator (must array of primatives): [map[hacker:1] OPEN]"),
		},
		{
			"in-array operator scalar value",
			nil,
			`{"status": {"$in": "text"}}`,
			``,
			nil,
			fmt.Errorf("invalid value for $in operator (must array of primatives): text"),
		},
		{
			"in-array operator with null value",
			nil,
			`{"status": {"$in": ["guest", null]}}`,
			`("status" = ANY($1))`,
			[]any{[]any{"guest", nil}},
			nil,
		},
		{
			"or operator basic",
			nil,
			`{"$or": [{"name": "John"}, {"name": "Doe"}]}`,
			`(("name" = $1) OR ("name" = $2))`,
			[]any{"John", "Doe"},
			nil,
		},
		{
			"or operator complex",
			nil,
			`{"$or": [{"org": "poki", "admin": true}, {"age": {"$gte": 18}}]}`,
			`((("admin" = $1) AND ("org" = $2)) OR ("age" >= $3))`,
			[]any{true, "poki", float64(18)},
			nil,
		},
		{
			"nested or",
			nil,
			`{"$or": [{"$or": [{"name": "John"}, {"name": "Doe"}]}, {"name": "Jane"}]}`,
			`((("name" = $1) OR ("name" = $2)) OR ("name" = $3))`,
			[]any{"John", "Doe", "Jane"},
			nil,
		},
		{
			"or in the wrong place",
			nil,
			`{"foo": { "$or": [ "bar", "baz" ] }}`,
			``,
			nil,
			fmt.Errorf("$or as scalar operator not supported"),
		},
		{
			"$nor operator basic",
			nil,
			`{"$nor": [{"name": "John"}, {"name": "Doe"}]}`,
			`NOT (("name" = $1) OR ("name" = $2))`,
			[]any{"John", "Doe"},
			nil,
		},
		{
			"and operator basic",
			nil,
			`{"$and": [{"name": "John"}, {"version": 3}]}`,
			`(("name" = $1) AND ("version" = $2))`,
			[]any{"John", float64(3)},
			nil,
		},
		{
			"and operator in one object",
			nil,
			`{"$and": [{"name": "John", "version": 3}]}`,
			`(("name" = $1) AND ("version" = $2))`,
			[]any{"John", float64(3)},
			nil,
		},
		{
			"basic contains operator",
			nil,
			`{"name": {"$regex": "John"}}`,
			`("name" ~* $1)`,
			[]any{"John"},
			nil,
		},
		{
			"complex contains operator",
			nil,
			`{"$or": [{"name": {"$regex": "John"}}, {"name": {"$regex": "Jane"}}]}`,
			`(("name" ~* $1) OR ("name" ~* $2))`,
			[]any{"John", "Jane"},
			nil,
		},
		{
			"don't allow empty objects",
			nil,
			`{"name": {}}`,
			``,
			nil,
			fmt.Errorf("empty objects not allowed"),
		},
		{
			"don't allow empty arrays",
			nil,
			`{"$or": []}`,
			``,
			nil,
			fmt.Errorf("empty arrays not allowed"),
		},
		{
			"do allow empty $in arrays",
			nil,
			`{"status": {"$in": []}}`,
			`("status" = ANY($1))`,
			[]any{[]any{}},
			nil,
		},
		{
			"empty filter",
			nil,
			`{}`,
			`FALSE`,
			nil,
			nil,
		}, {
			"empty or conditions",
			nil,
			`{"$or": [{}, {}]}`,
			``,
			nil,
			fmt.Errorf("empty objects not allowed"),
		},
		{
			"sql injection",
			nil,
			`{"\"bla = 1 --": 1}`,
			``,
			nil,
			fmt.Errorf("invalid column name: \"bla = 1 --"),
		},
		{
			"$not operator",
			nil,
			`{"$not": {"name": "John"}}`,
			`(NOT COALESCE(("name" = $1), FALSE))`,
			[]any{"John"},
			nil,
		},
		{
			"$not in the wrong place",
			nil,
			`{"name": {"$not": {"$eq": "John"}}}`,
			``,
			nil,
			fmt.Errorf("$not as scalar operator not supported"),
		},
		{
			"$not with a scalar",
			nil,
			`{"$not": "John"}`,
			``,
			nil,
			fmt.Errorf("invalid value for $not operator (must be object): John"),
		},
		{
			"sql injection",
			nil,
			`{"\"bla = 1 --": 1}`,
			``,
			nil,
			fmt.Errorf("invalid column name: \"bla = 1 --"),
		},
		{
			"compare with array",
			nil,
			`{"items": [200, 300]}`,
			``,
			nil,
			fmt.Errorf("invalid comparison value (must be a primitive): [200 300]"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := filter.NewConverter(tt.option)
			conditions, values, err := c.Convert([]byte(tt.input), 1)
			if err != nil && (tt.err == nil || err.Error() != tt.err.Error()) {
				t.Errorf("Converter.Convert() error = %v, wantErr %v", err, tt.err)
				return
			}
			if err == nil && tt.err != nil {
				t.Errorf("Converter.Convert() error = nil, wantErr %v", tt.err)
				return
			}
			if conditions != tt.conditions {
				t.Errorf("Converter.Convert() conditions:\n%v\nwant:\n%v", conditions, tt.conditions)
			}
			if !reflect.DeepEqual(values, tt.values) {
				t.Errorf("Converter.Convert() values:\n%#v\nwant:\n%#v", values, tt.values)
			}
		})
	}
}

func TestConverter_Convert_startAtParameterIndex(t *testing.T) {
	c := filter.NewConverter()
	conditions, values, err := c.Convert([]byte(`{"name": "John", "password": "secret"}`), 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := `(("name" = $10) AND ("password" = $11))`; conditions != want {
		t.Errorf("Converter.Convert() conditions = %v, want %v", conditions, want)
	}
	if !reflect.DeepEqual(values, []any{"John", "secret"}) {
		t.Errorf("Converter.Convert() values = %v, want %v", values, []any{"John"})
	}

	_, _, err = c.Convert([]byte(`{"name": "John"}`), 0)
	if want := "startAtParameterIndex must be greater than 0"; err == nil || err.Error() != want {
		t.Errorf("Converter.Convert(..., 0) error = nil, wantErr %q", want)
	}

	_, _, err = c.Convert([]byte(`{"name": "John"}`), -123)
	if want := "startAtParameterIndex must be greater than 0"; err == nil || err.Error() != want {
		t.Errorf("Converter.Convert(..., -123) error = nil, wantErr %q", want)
	}

	_, _, err = c.Convert([]byte(`{"name": "John"}`), 1234551231231231231)
	if err != nil {
		t.Errorf("Converter.Convert(..., 1234551231231231231) error = %v, want nil", err)
	}
}

func TestConverter_WithEmptyCondition(t *testing.T) {
	c := filter.NewConverter(filter.WithEmptyCondition("TRUE"))
	conditions, values, err := c.Convert([]byte(`{}`), 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := "TRUE"; conditions != want {
		t.Errorf("Converter.Convert() conditions = %v, want %v", conditions, want)
	}
	if len(values) != 0 {
		t.Errorf("Converter.Convert() values = %v, want nil", values)
	}
}
