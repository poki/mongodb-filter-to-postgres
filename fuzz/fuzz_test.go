package fuzz

import (
	"encoding/json"
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
		`{"status": {"$nin": ["NEW", "OPEN"]}}`,
		`{"status": {"$in": "text"}}`,
		`{"status": {"$in": ["guest", null]}}`,
		`{"$or": [{"name": "John"}, {"name": "Doe"}]}`,
		`{"$or": [{"org": "poki", "admin": true}, {"age": {"$gte": 18}}]}`,
		`{"$or": [{"$or": [{"name": "John"}, {"name": "Doe"}]}, {"name": "Jane"}]}`,
		`{"foo": { "$or": [ "bar", "baz" ] }}`,
		`{"$nor": [{"name": "John"}, {"name": "Doe"}]}`,
		`{"$and": [{"name": "John"}, {"version": 3}]}`,
		`{"$and": [{"name": "John", "version": 3}]}`,
		`{"name": {"$regex": "John"}}`,
		`{"$or": [{"name": {"$regex": "John"}}, {"name": {"$regex": "Jane"}}]}`,
		`{"name": {}}`,
		`{"$or": []}`,
		`{"status": {"$in": []}}`,
		`{"$or": [{}, {}]}`,
		`{"\"bla = 1 --": 1}`,
		`{"$not": {"name": "John"}}`,
		`{"name": {"$not": {"$eq": "John"}}}`,
		`{"name": null}`,
		`{"name": {"$exists": false}}`,
	}
	for _, tc := range tcs {
		f.Add(tc, true)
		f.Add(tc, false)
	}

	f.Fuzz(func(t *testing.T, in string, jsonb bool) {
		options := []filter.Option{
			filter.WithArrayDriver(pq.Array),
		}
		if jsonb {
			options = append(options, filter.WithNestedJSONB("meta"))
		}
		c := filter.NewConverter(options...)
		conditions, _, err := c.Convert([]byte(in), 1)
		if err == nil && conditions != "" {
			j, err := pg_query.ParseToJSON("SELECT * FROM test WHERE 1 AND " + conditions)
			if err != nil {
				t.Fatalf("%q %q %v", in, conditions, err)
			}

			t.Log(j)

			var q struct {
				Stmts []struct {
					Stmt struct {
						SelectStmt struct {
							FromClause []struct {
								RangeVar struct {
									Relname string `json:"relname"`
								} `json:"RangeVar"`
							} `json:"fromClause"`

							WhereClause struct {
								BoolExpr struct {
									Boolop string `json:"boolop"`
									Args   []any  `json:"args"`
								} `json:"BoolExpr"`
							} `json:"whereClause"`
						} `json:"SelectStmt"`
					} `json:"stmt"`
				} `json:"stmts"`
			}
			if err := json.Unmarshal([]byte(j), &q); err != nil {
				t.Fatal(err)
			}
			if len(q.Stmts) != 1 {
				t.Fatal(conditions, "len(q.Stmts) != 1")
			}
			if len(q.Stmts[0].Stmt.SelectStmt.FromClause) != 1 {
				t.Fatal(conditions, "len(q.Stmts[0].Stmt.SelectStmt.FromClause) != 1")
			}
			if q.Stmts[0].Stmt.SelectStmt.FromClause[0].RangeVar.Relname != "test" {
				t.Fatal(conditions, "q.Stmts[0].Stmt.SelectStmt.FromClause[0].RangeVar.Relname != test")
			}
			if q.Stmts[0].Stmt.SelectStmt.WhereClause.BoolExpr.Boolop != "AND_EXPR" {
				t.Fatal(conditions, "q.Stmts[0].Stmt.SelectStmt.WhereClause.BoolExpr.Boolop != AND_EXPR")
			}
			if len(q.Stmts[0].Stmt.SelectStmt.WhereClause.BoolExpr.Args) != 2 {
				t.Fatal(conditions, "len(q.Stmts[0].Stmt.SelectStmt.WhereClause.BoolExpr.Args) != 2")
			}
			if strings.Contains(j, "CommentStmt") {
				t.Fatal(conditions, "CommentStmt found")
			}
		}
	})
}
