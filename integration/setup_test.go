package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	_ "github.com/lib/pq"
)

func setupPQ(t *testing.T) *sql.DB {
	t.Helper()

	var db *sql.DB
	setupDatabase(t, func(dsn string) error {
		var err error
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		return db.PingContext(ctx)
	})
	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func setupPGX(t *testing.T) *pgxpool.Pool {
	t.Helper()

	var db *pgxpool.Pool
	setupDatabase(t, func(dsn string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		var err error
		db, err = pgxpool.New(ctx, dsn)
		if err != nil {
			return err
		}
		return db.Ping(ctx)
	})
	t.Cleanup(func() {
		db.Close() //nolint:errcheck
	})

	return db
}

func setupDatabase(t *testing.T, connect func(string) error) {
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

	dsn := fmt.Sprintf("postgres://test:test@%s/test?sslmode=disable", resource.GetHostPort("5432/tcp"))

	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		return connect(dsn)
	}); err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Fatalf("Could not purge resource: %s", err)
		}
	})
}

// createPlayersTable create a players table with 10 players.
func createPlayersTable(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`
		CREATE TABLE players (
			"id" serial PRIMARY KEY,
			"name" text,
			"metadata" jsonb,
			"level" int,
			"class" text,
			"mount" text,
			"items" text[],
			"parents" int[]
		);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO players
			("id", "name",    "metadata",                                           "level", "class",    "mount",   "items",              "parents") VALUES
			(1,    'Alice',   '{"guild_id": 20, "pet": "dog"                    }', 10,      'warrior',  'horse',   '{}',                 '{40, 60}'),
			(2,    'Bob',	  '{"guild_id": 20, "pet": "cat", "keys": [1, 3]    }', 20,      'mage',     'horse',   '{}',                 '{20, 30}'),
			(3,    'Charlie', '{"guild_id": 30, "pet": "dog", "keys": [4, 6]    }', 30,      'rogue',    NULL,      '{}',                 '{30, 50}'),
			(4,    'David',   '{"guild_id": 30, "pet": "cat"                    }', 40,      'warrior',  NULL,      '{}',                 '{}'),
			(5,    'Eve',     '{"guild_id": 40, "pet": "dog", "hats": ["helmet"]}', 50,      'mage',     'griffon', '{"staff", "cloak"}', '{}'),
			(6,    'Frank',   '{"guild_id": 40, "pet": "cat", "hats": ["cap"]   }', 60,      'rogue',    'griffon', '{"dagger"}',         '{}'),
			(7,    'Grace',   '{"guild_id": 50, "pet": "dog"                    }', 70,      'warrior',  'dragon',  '{"sword"}',          '{}'),
			(8,    'Hank',    '{"guild_id": 50, "pet": "cat"                    }', 80,      'mage',     'dragon',  '{}',                 '{}'),
			(9,    'Ivy',     '{"guild_id": 60                                  }', 90,      'rogue',    'phoenix', '{}',                 '{}'),
			(10,   'Jack',    '{"guild_id": 60, "pet": null                     }', 100,     'warrior',  'phoenix', '{}',                 '{}');
	`); err != nil {
		t.Fatal(err)
	}
}
