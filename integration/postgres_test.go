package integration

import (
	"context"
	"errors"
	"reflect"
	"strings"
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

	c, _ := filter.NewConverter(
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

	conditions, values, err := c.Convert([]byte(in), 1)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT id
		FROM lobbies
		WHERE `+conditions+`;
	`, values...)
	if err != nil {
		t.Fatal(err)
	}
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
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

	c, _ := filter.NewConverter(filter.WithAllowAllColumns(), filter.WithArrayDriver(pq.Array))
	in := `{
		"role": { "$in": ["guest", "user"] }
	}`
	conditions, values, err := c.Convert([]byte(in), 1)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT id
		FROM users
		WHERE `+conditions+`;
	`, values...)
	if err != nil {
		t.Fatal(err)
	}
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
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

	c, _ := filter.NewConverter(filter.WithAllowAllColumns())
	in := `{
		"role": { "$in": ["guest", "user"] }
	}`
	conditions, values, err := c.Convert([]byte(in), 1)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(ctx, `
		SELECT id
		FROM users
		WHERE `+conditions+`;
	`, values...)
	if err != nil {
		t.Fatal(err)
	}
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(ids) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(ids))
	}
	if !reflect.DeepEqual(ids, []int{3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Fatalf("expected [3, 4, 5, 6, 7, 8, 9, 10], got %v", ids)
	}
}

func TestIntegration_BasicOperators(t *testing.T) {
	db := setupPQ(t)

	createPlayersTable(t, db)

	tests := []struct {
		name            string
		input           string
		expectedPlayers []int
		expectedError   error
	}{
		{
			`$gt`,
			`{"level": {"$gt": 50}}`,
			[]int{6, 7, 8, 9, 10},
			nil,
		},
		{
			`$gte`,
			`{"level": {"$gte": 50}}`,
			[]int{5, 6, 7, 8, 9, 10},
			nil,
		},
		{
			`$lt`,
			`{"level": {"$lt": 50}}`,
			[]int{1, 2, 3, 4},
			nil,
		},
		{
			`$lte`,
			`{"level": {"$lte": 50}}`,
			[]int{1, 2, 3, 4, 5},
			nil,
		},
		{
			`$eq`,
			`{"name": "Alice"}`,
			[]int{1},
			nil,
		},
		{
			`$ne`,
			`{"name": {"$eq": "Alice"}}`,
			[]int{1},
			nil,
		},
		{
			`$ne`,
			`{"name": {"$ne": "Alice"}}`,
			[]int{2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil,
		},
		{
			`$regex`,
			`{"name": {"$regex": "a.k$"}}`,
			[]int{6, 8, 10},
			nil,
		},
		{
			`unknown column`,
			`{"foobar": "admin"}`,
			[]int{},
			nil,
		},
		{
			`invalid value type int`,
			`{"level": "town1"}`, // Level is an integer column, but the value is a string.
			nil,
			errors.New(`pq: invalid input syntax for type integer: "town1"`),
		},
		{
			`invalid value type string`,
			`{"name": 123}`, // Name is a string column, but the value is an integer.
			[]int{},
			nil,
		},
		{
			`empty object`,
			`{}`, // Should return FALSE as the condition.
			[]int{},
			nil,
		},
		{
			"$gt with jsonb column",
			`{"guild_id": { "$gt": 40 }}`,
			[]int{7, 8, 9, 10},
			nil,
		},
		{
			"$nor",
			`{"$nor": [{"name": "Alice"}, {"name": "Bob"}]}`,
			[]int{3, 4, 5, 6, 7, 8, 9, 10},
			nil,
		},
		{
			`$not on jsonb column`,
			`{"$not": {"pet": "cat"}}`,
			[]int{1, 3, 5, 7, 9, 10},
			nil,
		},
		{
			`$not on normal column`,
			`{"$not": {"mount": "horse"}}`,
			[]int{3, 4, 5, 6, 7, 8, 9, 10},
			nil,
		},
		{
			`column equal to null`,
			`{"mount": null}`,
			[]int{3, 4},
			nil,
		},
		{
			`jsonb equal to null`,
			`{"pet": null}`,
			[]int{10},
			nil,
		},
		{
			`jsonb exists`,
			`{"pet": {"$exists": false}}`,
			[]int{9},
			nil,
		},
		{
			`jsonb exists`,
			`{"pet": {"$exists": true}}`,
			[]int{1, 2, 3, 4, 5, 6, 7, 8, 10},
			nil,
		},
		{
			"$in",
			`{"level": {"$in": [20, 30, 40]}}`,
			[]int{2, 3, 4},
			nil,
		},
		{
			"$nin",
			`{"level": {"$nin": [20, 30, 40]}}`,
			[]int{1, 5, 6, 7, 8, 9, 10},
			nil,
		},
		{
			"$elemMatch on normal column",
			`{"items": {"$elemMatch": {"$regex": "a"}}}`,
			[]int{5, 6},
			nil,
		},
		{
			"$elemMatch on jsonb column",
			`{"hats": {"$elemMatch": {"$regex": "a"}}}`,
			[]int{6},
			nil,
		},
		{
			"$elemMatch with a numeric column",
			`{"parents": {"$elemMatch": {"$gt": 40, "$lt": 60}}}`,
			[]int{3},
			nil,
		},
		{
			"$elemMatch with numeric jsonb column",
			`{"keys": {"$elemMatch": {"$gt": 5}}}`,
			[]int{3},
			nil,
		},
		{
			"$lt bug with jsonb column",
			`{"guild_id": {"$lt": 100}}`,
			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil,
		},
		{
			"$lt with null and jsonb column",
			`{"guild_id": {"$lt": null}}`,
			[]int{},
			nil,
		},
		{
			"string order comparison",
			`{"pet": {"$lt": "dog"}}`,
			[]int{2, 4, 6, 8},
			nil,
		},
		{
			"compare two fields",
			`{"level": {"$lt": { "$field": "guild_id" }}}`,
			[]int{1},
			nil,
		},
		{
			"compare two string fields",
			`{"name": {"$field": "pet"}}`,
			[]int{},
			nil,
		},
		{
			"compare two string fields with jsonb",
			`{"pet": {"$field": "class"}}`,
			[]int{3},
			nil,
		},
		{
			// This converts to: ("level" = "metadata"->>'guild_id')
			// This currently doesn't work, because we don't know the type of the columns.
			// 'level' is an integer column, 'guild_id' is a jsonb column which always gets converted to a string.
			"compare two numeric fields",
			`{"level": {"$field": "guild_id"}}`,
			nil,
			errors.New(`pq: operator does not exist: integer = text`),
		},
		{
			// This converts to: (("metadata"->>'pet')::numeric < "class")
			// This currently doesn't work, because we always convert < etc to a numeric comparison.
			// We don't know the type of the columns, so we can't convert it to a string comparison.
			"string order comparison with two fields",
			`{"pet": {"$lt": {"$field": "class"}}}`,
			nil,
			errors.New(`pq: operator does not exist: numeric < text`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := filter.NewConverter(filter.WithArrayDriver(pq.Array), filter.WithNestedJSONB("metadata", "name", "level", "class", "mount", "items", "parents"))
			conditions, values, err := c.Convert([]byte(tt.input), 1)
			if err != nil {
				t.Fatal(err)
			}

			t.Log(conditions, values)

			rows, err := db.Query(`
				SELECT id
				FROM players
				WHERE `+conditions+`;
			`, values...)
			if err != nil {
				if tt.expectedError == nil {
					t.Fatalf("unexpected error: %v", err)
				} else if !strings.Contains(err.Error(), tt.expectedError.Error()) {
					t.Fatalf("expected error %q, got %q", tt.expectedError, err)
				}
				return
			}
			players := []int{}
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				players = append(players, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(players, tt.expectedPlayers) {
				t.Fatalf("expected %v, got %v", tt.expectedPlayers, players)
			}
		})
	}
}

func TestIntegration_NestedJSONB(t *testing.T) {
	db := setupPQ(t)

	createPlayersTable(t, db)

	tests := []struct {
		name            string
		input           string
		expectedPlayers []int
	}{
		{
			"jsonb equals",
			`{"guild_id": 20}`,
			[]int{1, 2},
		},
		{
			"jsonb regex",
			`{"pet": {"$regex": "^.{3}$"}}`,
			[]int{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"excemption column",
			`{"name": "Alice"}`,
			[]int{1},
		},
		{
			"unknown column",
			`{"foobar": "admin"}`,
			[]int{}, // Will always default to the jsonb column and return no results since it doesn't exist.
		},
		{
			"invalid value",
			`{"guild_id": "dragon_slayers"}`, // Guild ID only contains integer values in the test data.
			[]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := filter.NewConverter(filter.WithArrayDriver(pq.Array), filter.WithNestedJSONB("metadata", "name", "level", "class"))
			conditions, values, err := c.Convert([]byte(tt.input), 1)
			if err != nil {
				t.Fatal(err)
			}

			rows, err := db.Query(`
				SELECT id
				FROM players
				WHERE `+conditions+`;
			`, values...)
			if err != nil {
				t.Fatal(err)
			}
			players := []int{}
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				players = append(players, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(players, tt.expectedPlayers) {
				t.Fatalf("%q expected %v, got %v (conditions used: %q)", tt.input, tt.expectedPlayers, players, conditions)
			}
		})
	}
}

func TestIntegration_Logic(t *testing.T) {
	db := setupPQ(t)

	createPlayersTable(t, db)

	tests := []struct {
		name            string
		input           string
		expectedPlayers []int
	}{
		{
			"basic or",
			`{"$or": [{"level": {"$gt": 50}}, {"pet": "dog"}]}`,
			[]int{1, 3, 5, 6, 7, 8, 9, 10},
		},
		{
			// (mages and (ends with E or ends with K)) or (dog owners and (guild in (50, 20)))
			"complex triple nested",
			`{"$or": [
				{"$and": [
					{"class": "mage"},
					{"$or": [
						{"name": {"$regex": "e$"}},
						{"name": {"$regex": "k$"}}
					]}
				]},
				{"$and": [
					{"pet": "dog"},
					{"guild_id": {"$in": [50, 20]}}
				]}
			]}`,
			[]int{1, 5, 7, 8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := filter.NewConverter(filter.WithArrayDriver(pq.Array), filter.WithNestedJSONB("metadata", "name", "level", "class"))
			conditions, values, err := c.Convert([]byte(tt.input), 1)
			if err != nil {
				t.Fatal(err)
			}

			rows, err := db.Query(`
				SELECT id
				FROM players
				WHERE `+conditions+`;
			`, values...)
			if err != nil {
				t.Fatal(err)
			}
			players := []int{}
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				players = append(players, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(players, tt.expectedPlayers) {
				t.Fatalf("%q expected %v, got %v (conditions used: %q)", tt.input, tt.expectedPlayers, players, conditions)
			}
		})
	}
}
