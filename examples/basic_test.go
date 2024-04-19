package examples

import (
	"fmt"

	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func ExampleNewConverter() {
	converter := filter.NewConverter(filter.WithNestedJSONB("meta", "created_at", "updated_at"))

	mongoFilterQuery := `{
		"name": "John",
		"created_at": {
			"$gte": "2020-01-01T00:00:00Z"
		}
	}`
	conditions, values, err := converter.Convert([]byte(mongoFilterQuery))
	if err != nil {
		// handle error
	}

	fmt.Println(conditions)
	fmt.Printf("%#v\n", values)
	// Output:
	// (("created_at" >= $1) AND ("meta"->>'name' = $2))
	// []interface {}{"2020-01-01T00:00:00Z", "John"}
}
