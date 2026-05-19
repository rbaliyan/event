//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq" // pq driver registered as side effect for sql.Open
)

// PostgresDSNEnv is the environment variable consulted for the Postgres DSN.
const PostgresDSNEnv = "POSTGRES_DSN"

// defaultPostgresDSN is used when PostgresDSNEnv is unset. The CI
// service-container pattern and the standard local docker compose both expose
// Postgres at 5432; the user/db `test` defaults match the existing
// idempotency integration test's DSN.
const defaultPostgresDSN = "postgres://localhost:5432/test?sslmode=disable"

// SetupPostgres returns a *sql.DB whose search_path is pinned to a
// per-run schema. If Postgres is unreachable the test is skipped. The
// per-run schema is DROPed on teardown, so even tests that CREATE TABLE
// without per-run namespacing produce no cross-test contamination.
//
// Tests that need to operate against the public schema (e.g. those exercising
// CreateTable idempotency on a fixed name) should drop the schema_search via
// `db.ExecContext(ctx, "SET search_path TO public")` inside the subtest —
// the cleanup still drops the per-run schema, so nothing leaks.
func SetupPostgres(t testing.TB) (*sql.DB, string) {
	t.Helper()

	dsn := os.Getenv(PostgresDSNEnv)
	if dsn == "" {
		dsn = defaultPostgresDSN
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open(postgres): %v", err) // misconfigured DSN, not "unreachable"
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		t.Skipf("Postgres unreachable at %s (%s=%s): %v",
			dsn, PostgresDSNEnv, os.Getenv(PostgresDSNEnv), err)
	}

	schema := "test_" + strings.ToLower(UniqueName(t))

	createCtx, createCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer createCancel()
	if _, err := db.ExecContext(createCtx, fmt.Sprintf(`CREATE SCHEMA %q`, schema)); err != nil {
		_ = db.Close()
		t.Fatalf("CREATE SCHEMA %q: %v", schema, err)
	}

	// Pin search_path at the connection-pool level so every subsequent query
	// resolves identifiers within the per-run schema by default. Tests that
	// need to override can SET search_path themselves.
	if _, err := db.ExecContext(createCtx, fmt.Sprintf(`SET search_path TO %q`, schema)); err != nil {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
		_ = db.Close()
		t.Fatalf("SET search_path: %v", err)
	}

	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		if _, err := db.ExecContext(dropCtx, fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema)); err != nil {
			t.Logf("postgres cleanup DROP SCHEMA %q: %v", schema, err)
		}
		_ = db.Close()
	})

	return db, schema
}
