package fuzz

import (
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/poki/mongodb-filter-to-postgres/filter"

	pg_query "github.com/pganalyze/pg_query_go/v5"
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
		c := filter.NewConverter(filter.WithArrayDriver(pq.Array))
		conditions, _, err := c.Convert([]byte(in), 1)
		if err == nil && conditions != "" {
			j, err := pg_query.ParseToJSON("SELECT * FROM test WHERE 1 AND " + conditions)
			if err != nil {
				t.Fatalf("%q %q %v", in, conditions, err)
			}

			if strings.Contains(j, "CommentStmt") {
				t.Fatal(conditions, "CommentStmt found")
			}
		}
	})
}
