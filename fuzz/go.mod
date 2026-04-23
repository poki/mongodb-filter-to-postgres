module github.com/poki/mongodb-filter-to-postgres/fuzz

go 1.22

toolchain go1.25.9

replace github.com/poki/mongodb-filter-to-postgres v0.0.0 => ../

require (
	github.com/lib/pq v1.10.9
	github.com/pganalyze/pg_query_go/v6 v6.1.0
	github.com/poki/mongodb-filter-to-postgres v0.0.0
)

require google.golang.org/protobuf v1.36.6 // indirect
