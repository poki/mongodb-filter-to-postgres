package integration

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/poki/mongodb-filter-to-postgres/filter"
)

func TestIntegration_ReadmeExample(t *testing.T) {
	db := SetupDatabase(t)
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

	c := filter.NewConverter(filter.WithNestedJSONB("customData", "password", "playerCount"))
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
	if err != nil {
		t.Fatal(err)
	}

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

func SetupDatabase(t *testing.T) *sql.DB {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		t.Fatalf("Could not connect to Docker: %s", err)
	}
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15-alpine",
		Env: []string{
			"POSTGRES_PASSWORD=test",
			"POSTGRES_USER=test",
			"POSTGRES_DB=test",
			"listen_addresses='*'",
			"fsync='off'",
			"full_page_writes='off'",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	resource.Expire(120) //nolint:errcheck

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://test:test@%s/test?sslmode=disable", hostAndPort)

	var db *sql.DB
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		db, err = sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		return db.PingContext(ctx)
	}); err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Fatalf("Could not purge resource: %s", err)
		}
	})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Could not close database: %s", err)
		}
	})

	return db
}
