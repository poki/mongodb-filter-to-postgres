package examples

import (
	"fmt"

	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func ExampleNewConverter_README() {
	converter := filter.NewConverter(filter.WithNestedJSONB("meta", "created_at", "updated_at"))

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
	conditions, values, err := converter.Convert([]byte(mongoFilterQuery))
	if err != nil {
		panic(err)
		// handle error
	}

	fmt.Println(conditions)
	fmt.Printf("%#v\n", values)
	// Output:
	// ((("meta"->>'map' ~* $1) OR ("meta"->>'map' ~* $2)) AND ("meta"->>'password' = $3) AND (("meta"->>'playerCount' >= $4) AND ("meta"->>'playerCount' < $5)))
	// []interface {}{"aztec", "nuke", "", 2, 10}
}
