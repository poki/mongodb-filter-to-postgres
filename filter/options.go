package filter

import (
	"database/sql"
	"database/sql/driver"
)

type Option func(*Converter)

// WithNestedJSONB is an option to specify the column name that contains the nested
// JSONB object. (e.g. you have a column named `metadata` that contains a nested
// JSONB object)
//
// When this option is set, all keys in the query will be directed to the nested
// column, you can exempt some keys by providing them as the second argument.
//
// Example:
//
//	c := filter.NewConverter(filter.WithNestedJSONB("metadata", "created_at", "updated_at"))
func WithNestedJSONB(column string, exemption ...string) Option {
	return func(c *Converter) {
		c.nestedColumn = column
		c.nestedExemptions = exemption
	}
}

// WithArrayDriver is an option to specify a custom driver to convert array values
// to Postgres driver compatible types.
// An example for github.com/lib/pq is:
//
//	c := filter.NewConverter(filter.WithArrayDriver(pq.Array))
//
// For github.com/jackc/pgx this option is not needed.
func WithArrayDriver(f func(a any) interface {
	driver.Valuer
	sql.Scanner
}) Option {
	return func(c *Converter) {
		c.arrayDriver = f
	}
}

// WithEmptyCondition is an option to specify the condition to be used when the
// input query filter is empty. (e.g. you have a query with no conditions)
//
// The default value is `FALSE`, because it's the safer choice in most cases.
func WithEmptyCondition(condition string) Option {
	return func(c *Converter) {
		c.emptyCondition = condition
	}
}
