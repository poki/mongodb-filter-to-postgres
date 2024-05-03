package examples

import (
	"database/sql"
	"fmt"

	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func ExampleNewConverter() {
	// Remeber to use `filter.WithArrayDriver(pg.Array)` when using github.com/lib/pq
	converter := filter.NewConverter(filter.WithNestedJSONB("meta", "created_at", "updated_at"))

	mongoFilterQuery := `{
		"name": "John",
		"created_at": {
			"$gte": "2020-01-01T00:00:00Z"
		}
	}`
	conditions, values, err := converter.Convert([]byte(mongoFilterQuery), 1)
	if err != nil {
		// handle error
	}

	fmt.Println(conditions)
	fmt.Printf("%#v\n", values)
	// Output:
	// (("created_at" >= $1) AND ("meta"->>'name' = $2))
	// []interface {}{"2020-01-01T00:00:00Z", "John"}
}

func ExampleNewConverter_emptyfilter() {
	converter := filter.NewConverter(filter.WithEmptyCondition("TRUE")) // The default is FALSE if you don't change it.

	mongoFilterQuery := `{}`
	conditions, _, err := converter.Convert([]byte(mongoFilterQuery), 1)
	if err != nil {
		// handle error
	}

	fmt.Println(conditions)
	// Output:
	// TRUE
}

func ExampleNewConverter_nonIsolatedConditions() {
	converter := filter.NewConverter()

	mongoFilterQuery := `{
		"$or": [
			{ "email": "johndoe@example.org" },
			{ "name": {"$regex": "^John.*^" },
		]
	}`
	conditions, values, err := converter.Convert([]byte(mongoFilterQuery), 3)
	if err != nil {
		// handle error
	}

	query := `
		SELECT *
		FROM users
		WHERE
			disabled_at IS NOT NULL
			AND role = $1
			AND verified_at > $2
			AND ` + conditions + `
		LIMIT 10
	`

	role := "user"
	verifiedAt := "2020-01-01T00:00:00Z"
	values = append([]any{role, verifiedAt}, values...)

	db, _ := sql.Open("postgres", "...")
	rows := db.QueryRow(query, values...)
	_ = rows // actually use rows
}
