package filter

import (
	"database/sql"
	"database/sql/driver"
)

type Option struct {
	f              func(*Converter)
	isAccessOption bool
}

// WithAllowAllColumns is the option to allow all columns in the query.
func WithAllowAllColumns() Option {
	return Option{
		f: func(c *Converter) {
			c.allowAllColumns = true
		},
		isAccessOption: true,
	}
}

// WithAllowColumns is an option to allow only the specified columns in the query.
func WithAllowColumns(columns ...string) Option {
	return Option{
		f: func(c *Converter) {
			c.allowedColumns = append(c.allowedColumns, columns...)
		},
		isAccessOption: true,
	}
}

// WithDisallowColumns is an option to disallow the specified columns in the query.
func WithDisallowColumns(columns ...string) Option {
	return Option{
		f: func(c *Converter) {
			c.disallowedColumns = append(c.disallowedColumns, columns...)
		},
		isAccessOption: true,
	}
}

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
	return Option{
		f: func(c *Converter) {
			c.nestedColumn = column
			c.nestedExemptions = exemption
		},
		isAccessOption: true,
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
	return Option{
		f: func(c *Converter) {
			c.arrayDriver = f
		},
	}
}

// WithEmptyCondition is an option to specify the condition to be used when the
// input query filter is empty. (e.g. you have a query with no conditions)
//
// The default value is `FALSE`, because it's the safer choice in most cases.
func WithEmptyCondition(condition string) Option {
	return Option{
		f: func(c *Converter) {
			c.emptyCondition = condition
		},
	}
}

// WithPlaceholderName is an option to specify the placeholder name that will be
// used in the generated SQL query. This name should not be used in the database
// or any JSONB column.
func WithPlaceholderName(name string) Option {
	return Option{
		f: func(c *Converter) {
			c.placeholderName = name
		},
	}
}
