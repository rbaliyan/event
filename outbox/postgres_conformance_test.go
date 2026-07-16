package outbox

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestPostgresStoreConformance(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	// Ensure the schema exists (idempotent) — the outbox table is otherwise
	// only documented in the package comment, so a fresh integration database
	// has no event_outbox table to truncate.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS event_outbox (
			id           BIGSERIAL PRIMARY KEY,
			event_name   VARCHAR(255) NOT NULL,
			event_id     VARCHAR(36) NOT NULL,
			payload      BYTEA NOT NULL,
			metadata     JSONB,
			created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
			published_at TIMESTAMP,
			status       VARCHAR(20) NOT NULL DEFAULT 'pending',
			retry_count  INT NOT NULL DEFAULT 0,
			last_error   TEXT,
			priority     INT NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox(status, priority DESC, created_at) WHERE status IN ('pending', 'failed');
	`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE event_outbox`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	store, err := NewPostgresStore(db)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	seed := func(ctx context.Context, eventID string) error {
		return store.Store(ctx, "conf.event", eventID, []byte(`{}`), nil)
	}
	RunStoreConformance(t, ctx, store, seed)
}
