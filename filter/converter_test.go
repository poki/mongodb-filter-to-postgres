package filter_test

import (
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
			`("status" IN ($1, $2))`,
			[]any{"NEW", "OPEN"},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := filter.NewConverter(tt.option)
			conditions, values, err := c.Convert([]byte(tt.input))
			if err != tt.err {
				t.Errorf("Converter.Convert() error = %v, wantErr %v", err, tt.err)
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
