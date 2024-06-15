module github.com/poki/mongodb-filter-to-postgres/fuzz

go 1.18

replace github.com/poki/mongodb-filter-to-postgres v0.0.0 => ../

require (
	github.com/lib/pq v1.10.9
	github.com/pganalyze/pg_query_go/v5 v5.1.0
	github.com/poki/mongodb-filter-to-postgres v0.0.0
)

require google.golang.org/protobuf v1.33.0 // indirect
