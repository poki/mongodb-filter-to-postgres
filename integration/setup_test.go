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
