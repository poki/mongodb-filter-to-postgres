package integration

import (
	"context"
	"reflect"
	"testing"

	"github.com/lib/pq"
	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func TestIntegration_ReadmeExample(t *testing.T) {
	db := setupPQ(t)

	if _, err := db.Exec(`
		CREATE TABLE lobbies (
			"id" serial PRIMARY KEY,
			"password" text,
			"playerCount" int,
			"customData" jsonb
		);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO lobbies ("id", "password", "playerCount", "customData")
		VALUES
			(1, 'password', 0, '{"map": "aztec"}'),
			(2, '', 4, '{"map": "nuke"}'),
			(3, '', 2, '{"map": "dust2"}'),
			(4, 'password', 3, '{"map": "inferno"}'),
			(5, '', 4, '{"map": "vertigo"}'),
			(6, '', 1, '{"map": "nuke"}'),
			(7, 'password', 6, '{"map": "overpass"}'),
			(8, '', 7, '{"map": "aztec"}'),
			(9, '', 8, '{"map": "cobblestone"}'),
			(10, 'password', 9, '{"map": "agency"}')
	`); err != nil {
		t.Fatal(err)
	}

	c := filter.NewConverter(
		filter.WithArrayDriver(pq.Array),
		filter.WithNestedJSONB("customData", "password", "playerCount"),
	)
	in := `{
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

	where, values, err := c.Convert([]byte(in))

	rows, err := db.Query(`
		SELECT id
		FROM lobbies
		WHERE `+where+`;
	`, values...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	if len(ids) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(ids))
	}
	if !reflect.DeepEqual(ids, []int{2, 8}) {
		t.Fatalf("expected [2, 8], got %v", ids)
	}
}

func TestIntegration_InAny_PQ(t *testing.T) {
	db := setupPQ(t)

	if _, err := db.Exec(`
		CREATE TABLE users (
			"id" serial PRIMARY KEY,
			"name" text,
			"role" text
		);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO users ("id", "name", "role")
		VALUES
			(1, 'Alice', 'admin'),
			(2, 'Bob', 'admin'),
			(3, 'Charlie', 'guest'),
			(4, 'David', 'user'),
			(5, 'Eve', 'user'),
			(6, 'Frank', 'guest'),
			(7, 'Grace', 'user'),
			(8, 'Hank', 'user'),
			(9, 'Ivy', 'guest'),
			(10, 'Jack', 'user')
	`); err != nil {
		t.Fatal(err)
	}

	c := filter.NewConverter(filter.WithArrayDriver(pq.Array))
	in := `{
		"role": { "$in": ["guest", "user"] }
	}`
	where, values, err := c.Convert([]byte(in))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT id
		FROM users
		WHERE `+where+`;
	`, values...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	if len(ids) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(ids))
	}
	if !reflect.DeepEqual(ids, []int{3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Fatalf("expected [3, 4, 5, 6, 7, 8, 9, 10], got %v", ids)
	}
}

func TestIntegration_InAny_PGX(t *testing.T) {
	db := setupPGX(t)

	ctx := context.Background()
	if _, err := db.Exec(ctx, `
		CREATE TABLE users (
			"id" serial PRIMARY KEY,
			"name" text,
			"role" text
		);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO users ("id", "name", "role")
		VALUES
			(1, 'Alice', 'admin'),
			(2, 'Bob', 'admin'),
			(3, 'Charlie', 'guest'),
			(4, 'David', 'user'),
			(5, 'Eve', 'user'),
			(6, 'Frank', 'guest'),
			(7, 'Grace', 'user'),
			(8, 'Hank', 'user'),
			(9, 'Ivy', 'guest'),
			(10, 'Jack', 'user')
	`); err != nil {
		t.Fatal(err)
	}

	c := filter.NewConverter()
	in := `{
		"role": { "$in": ["guest", "user"] }
	}`
	where, values, err := c.Convert([]byte(in))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(ctx, `
		SELECT id
		FROM users
		WHERE `+where+`;
	`, values...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	if len(ids) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(ids))
	}
	if !reflect.DeepEqual(ids, []int{3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Fatalf("expected [3, 4, 5, 6, 7, 8, 9, 10], got %v", ids)
	}
}
