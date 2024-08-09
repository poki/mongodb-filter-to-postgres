# mongodb-filter-to-postgres

This package converts [MongoDB query filters](https://www.mongodb.com/docs/compass/current/query/filter) into PostgreSQL WHERE clauses.  
_It's designed to be simple, secure, and free of dependencies._

### Why Use This Package?
When filtering data based on user-generated inputs, you need a syntax that's both intuitive and reliable. MongoDB's query filter is an excellent choice because it's simple, widely understood, and battle-tested in real-world applications. Although this package doesn't interact with MongoDB, it uses the same syntax to simplify filtering.

### Supported Features:
- Basics: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$regex`, `$exists`
- Logical operators: `$and`, `$or`, `$not`, `$nor`
- Array operators: `$in`, `$nin`, `$elemMatch`
- Field comparison: `$field` (see [#difference-with-mongodb](#difference-with-mongodb))

This package is intended for use with PostgreSQL drivers like [github.com/lib/pq](https://github.com/lib/pq) and [github.com/jackc/pgx](https://github.com/jackc/pgx). However, it can work with any driver that supports the database/sql package.


## Basic Usage:

Install the package in your project:
```sh
go get -u github.com/poki/mongodb-filter-to-postgres
```

Basic example:
```go
import (
  "github.com/poki/mongodb-filter-to-postgres/filter"

  "github.com/lib/pq" // also works with github.com/jackc/pgx
)

func main() {
  // Create a converter with options:
  // - WithArrayDriver: to convert arrays to the correct driver type, required when using lib/pq
  converter := filter.NewConverter(filter.WithArrayDriver(pq.Array))

  // Convert a filter query to a WHERE clause and values:
  input := []byte(`{"title": "Jurassic Park"}`)
  conditions, values, err := converter.Convert(input, 1) // 1 is the starting index for params, $1, $2, ...
  if err != nil {
    // handle error
  }
  fmt.Println(conditions, values) // ("title" = $1), ["Jurassic Park"]

  db, _ := sql.Open("postgres", "...")
  db.QueryRow("SELECT * FROM movies WHERE " + conditions, values...)
}
```
(See [examples/](examples/) for more examples)


## Complex filter example:

This project was created and designed for the
[poki/netlib](https://github.com/poki/netlib) project, where we needed to
convert complex filters from the API to SQL queries. The following example
shows a complex filter and how it is converted to a WHERE clause and values:

```json5
{
  "$and": [
    {
      "$or": [                                // match two maps
        { "map": { "$regex": "aztec" } },
        { "map": { "$regex": "nuke" } }
      ]
    },
    { "password": "" },                       // no password set
    {
      "playerCount": { "$gte": 2, "$lt": 10 } // not empty or full
    }
  ]
}
```
Converts to:
```sql
(
  "customdata"->>"map" ~* $1
  OR
  "customdata"->>"map" ~* $2
)
AND "password" = $3
AND (
  "playerCount" >= $4
  AND
  "playerCount" < $5
)
```
```go
values := []any{"aztec", "nuke", "", 2, 10}
```
(given "customdata" is configured with `filter.WithNestedJSONB("customdata", "password", "playerCount")`)


## Difference with MongoDB

The MongoDB query filters don't have the option to compare fields with each other. This package adds the `$field` operator to compare fields with each other.

For example:
```json5
{
  "playerCount": { "$lt": { "$field": "maxPlayers" } }
}
```


## Contributing

If you have a feature request or discovered a bug, we'd love to hear from you! Please open an issue or submit a pull request. This project adheres to the [Poki Vulnerability Disclosure Policy](https://poki.com/en/c/vulnerability-disclosure-policy).

## Main Contributors

- [Koen Bollen](https://github.com/koenbollen)
- [Erik Dubbelboer](https://github.com/erikdubbelboer)
