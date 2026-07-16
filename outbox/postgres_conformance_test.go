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
