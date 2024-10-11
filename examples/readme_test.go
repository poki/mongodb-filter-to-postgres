package examples

import (
	"fmt"

	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func ExampleNewConverter_readme() {
	// Remeber to use `filter.WithArrayDriver(pg.Array)` when using github.com/lib/pq
	converter, err := filter.NewConverter(filter.WithNestedJSONB("meta", "created_at", "updated_at"))
	if err != nil {
		// handle error
	}

	mongoFilterQuery := `{
		"$and": [
			{
				"$or": [
					{ "map": { "$regex": "aztec" } },
					{ "map": { "$regex": "nuke" } }
				]
			},
			{ "password": "" },
			{
				"playerCount": { "$gte": 2, "$lt": 10 }
			}
		]
	}`
	conditions, values, err := converter.Convert([]byte(mongoFilterQuery), 1)
	if err != nil {
		// handle error
		panic(err)
	}

	fmt.Println(conditions)
	fmt.Printf("%#v\n", values)
	// Output:
	// ((("meta"->>'map' ~* $1) OR ("meta"->>'map' ~* $2)) AND ("meta"->>'password' = $3) AND ((("meta"->>'playerCount')::numeric >= $4) AND (("meta"->>'playerCount')::numeric < $5)))
	// []interface {}{"aztec", "nuke", "", 2, 10}
}
