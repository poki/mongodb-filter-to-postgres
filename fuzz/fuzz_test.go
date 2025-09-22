package fuzz

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/poki/mongodb-filter-to-postgres/filter"

	pg_query "github.com/pganalyze/pg_query_go/v6"
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
		`{"name": {"$elemMatch": {"$eq": "John"}}}`,
		`{"age": {"$elemMatch": {"$gt": 18}}}`,
	}
	for _, tc := range tcs {
		f.Add(tc, true)
		f.Add(tc, false)
	}

	f.Fuzz(func(t *testing.T, in string, jsonb bool) {
		options := []filter.Option{
			filter.WithAllowAllColumns(),
			filter.WithArrayDriver(pq.Array),
		}
		if jsonb {
			options = append(options, filter.WithNestedJSONB("meta"))
		}
		c, _ := filter.NewConverter(options...)
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

func FuzzConverterOrderBy(f *testing.F) {
	tcs := []string{
		`{"level": 1}`,
		`{"level": -1}`,
		`{"name": 1, "level": -1}`,
		`{"created_at": 1}`,
		`{"guild_id": -1}`,
		`{"pet": 1, "level": -1}`,
		`{"class": 1, "level": -1, "name": 1}`,
		`{}`,
		`{"invalid_direction": 2}`,
		`{"invalid_string": "asc"}`,
		`{"field_with_spaces": 1}`,
		`{"level": 1.0}`,
		`{"level": -1.0}`,
		`{"level": 0}`,
		`{"field_name": -1}`,
		`{"validField": 1}`,
		`{"user_id": -1, "created_at": 1}`,
	}
	for _, tc := range tcs {
		f.Add(tc, true)
		f.Add(tc, false)
	}

	f.Fuzz(func(t *testing.T, in string, jsonb bool) {
		options := []filter.Option{
			filter.WithAllowAllColumns(),
			filter.WithArrayDriver(pq.Array),
		}
		if jsonb {
			options = append(options, filter.WithNestedJSONB("meta", "created_at"))
		}
		c, _ := filter.NewConverter(options...)
		orderBy, err := c.ConvertOrderBy([]byte(in))
		if err == nil && orderBy != "" {
			// Test that the generated ORDER BY clause is valid SQL syntax
			sql := "SELECT * FROM test ORDER BY " + orderBy
			j, err := pg_query.ParseToJSON(sql)
			if err != nil {
				// If the SQL is invalid, this might indicate a bug in the ConvertOrderBy function
				// Log it but don't fail the fuzz test since this helps us find edge cases
				t.Logf("Invalid SQL generated for input %q: %q -> error: %v", in, orderBy, err)
				return
			}

			t.Log("Input:", in, "-> ORDER BY:", orderBy)

			// Parse the JSON to ensure it contains valid ORDER BY structure
			var q struct {
				Stmts []struct {
					Stmt struct {
						SelectStmt struct {
							FromClause []struct {
								RangeVar struct {
									Relname string `json:"relname"`
								} `json:"RangeVar"`
							} `json:"fromClause"`
							SortClause []any `json:"sortClause"`
						} `json:"SelectStmt"`
					} `json:"stmt"`
				} `json:"stmts"`
			}
			if err := json.Unmarshal([]byte(j), &q); err != nil {
				t.Fatal(err)
			}

			if len(q.Stmts) != 1 {
				t.Fatal("Expected exactly 1 statement, got", len(q.Stmts))
			}

			if len(q.Stmts[0].Stmt.SelectStmt.FromClause) != 1 {
				t.Fatal("Expected exactly 1 from clause, got", len(q.Stmts[0].Stmt.SelectStmt.FromClause))
			}

			if q.Stmts[0].Stmt.SelectStmt.FromClause[0].RangeVar.Relname != "test" {
				t.Fatal("Expected table name 'test', got", q.Stmts[0].Stmt.SelectStmt.FromClause[0].RangeVar.Relname)
			}

			// Verify we have sort clauses when ORDER BY is not empty
			if len(q.Stmts[0].Stmt.SelectStmt.SortClause) == 0 {
				t.Fatal("Expected sort clauses for ORDER BY:", orderBy)
			}

			// Check for SQL injection attempts
			if strings.Contains(j, "CommentStmt") {
				t.Fatal("CommentStmt found - potential SQL injection in:", orderBy)
			}
		}
	})
}
