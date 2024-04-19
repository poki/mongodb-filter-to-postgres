package filter_test

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func FuzzConverter(f *testing.F) {
	tcs := []string{
		`{"name": "John"}`,
		`{"age": 30, "name": "John"}`,
		`{"players": {"$gt": 0}}`,
		`{"age": {"$gte": 18}, "name": "John"}`,
		`{"created_at": {"$gte": "2020-01-01T00:00:00Z"}, "name": "John", "role": "admin"}`,
		`{"b": 1, "c": 2, "a": 3}`,
		`{"status": {"$in": ["NEW", "OPEN"]}}`,
		`{"status": {"$in": [{"hacker": 1}, "OPEN"]}}`,
		`{"status": {"$in": "text"}}`,
		`{"status": {"$in": ["guest", null]}}`,
		`{"$or": [{"name": "John"}, {"name": "Doe"}]}`,
		`{"$or": [{"org": "poki", "admin": true}, {"age": {"$gte": 18}}]}`,
		`{"$or": [{"$or": [{"name": "John"}, {"name": "Doe"}]}, {"name": "Jane"}]}`,
		`{"foo": { "$or": [ "bar", "baz" ] }}`,
		`{"$and": [{"name": "John"}, {"version": 3}]}`,
		`{"$and": [{"name": "John", "version": 3}]}`,
		`{"name": {"$regex": "John"}}`,
		`{"$or": [{"name": {"$regex": "John"}}, {"name": {"$regex": "Jane"}}]}`,
		`{"name": {}}`,
		`{"$or": []}`,
		`{"status": {"$in": []}}`,
	}
	for _, tc := range tcs {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, in string) {
		c := filter.NewConverter()
		where, _, err := c.Convert([]byte(in))
		if err == nil && where != "" {
			j, err := pg_query.ParseToJSON("SELECT * FROM test WHERE 1 AND " + where)
			if err != nil {
				t.Fatalf("%q %q %v", in, where, err)
			}

			if strings.Contains(j, "CommentStmt") {
				t.Fatal(where, "CommentStmt found")
			}
		}
	})
}
