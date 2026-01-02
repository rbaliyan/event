package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// PostgresProvider implements SchemaProvider using PostgreSQL.
//
// Table Schema:
//
//	CREATE TABLE event_schemas (
//	    name TEXT PRIMARY KEY,
//	    version INT NOT NULL DEFAULT 1,
//	    description TEXT,
//	    sub_timeout_ms BIGINT,
//	    max_retries INT,
//	    retry_backoff_ms BIGINT,
//	    enable_monitor BOOLEAN DEFAULT false,
//	    enable_idempotency BOOLEAN DEFAULT false,
//	    enable_poison BOOLEAN DEFAULT false,
//	    metadata JSONB,
//	    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
//	);
//
// Example:
//
//	db, _ := sql.Open("postgres", connString)
//	provider := schema.NewPostgresProvider(db, publishFunc)
//	defer provider.Close()
type PostgresProvider struct {
	db        *sql.DB
	tableName string
	publisher func(context.Context, SchemaChangeEvent) error
	watchers  []chan SchemaChangeEvent
	mu        sync.RWMutex
	closed    bool
	closeChan chan struct{}
}

// PostgresOption configures PostgresProvider.
type PostgresOption func(*PostgresProvider)

// WithTableName sets a custom table name (default: "event_schemas").
func WithTableName(name string) PostgresOption {
	return func(p *PostgresProvider) {
		p.tableName = name
	}
}

// NewPostgresProvider creates a new PostgreSQL-based schema provider.
//
// The publisher callback is called when a schema is set, to notify
// subscribers via the transport. It can be nil if Watch is not needed.
func NewPostgresProvider(db *sql.DB, publisher func(context.Context, SchemaChangeEvent) error, opts ...PostgresOption) *PostgresProvider {
	p := &PostgresProvider{
		db:        db,
		tableName: "event_schemas",
		publisher: publisher,
		closeChan: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Get retrieves a schema by event name.
func (p *PostgresProvider) Get(ctx context.Context, eventName string) (*EventSchema, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrProviderClosed
	}
	p.mu.RUnlock()

	query := fmt.Sprintf(`
		SELECT name, version, description, sub_timeout_ms, max_retries, retry_backoff_ms,
		       enable_monitor, enable_idempotency, enable_poison, metadata,
		       created_at, updated_at
		FROM %s
		WHERE name = $1
	`, p.tableName)

	row := p.db.QueryRowContext(ctx, query, eventName)
	schema, err := p.scanRow(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	return schema, nil
}

// Set stores a schema and notifies subscribers.
func (p *PostgresProvider) Set(ctx context.Context, schema *EventSchema) error {
	if err := schema.Validate(); err != nil {
		return err
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProviderClosed
	}
	p.mu.RUnlock()

	// Check version
	existing, err := p.Get(ctx, schema.Name)
	if err != nil {
		return err
	}
	if existing != nil && schema.Version < existing.Version {
		return fmt.Errorf("%w: %d < %d", ErrVersionDowngrade, schema.Version, existing.Version)
	}

	// Serialize metadata
	var metadataJSON []byte
	if schema.Metadata != nil {
		metadataJSON, err = json.Marshal(schema.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			name, version, description, sub_timeout_ms, max_retries, retry_backoff_ms,
			enable_monitor, enable_idempotency, enable_poison, metadata,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
		ON CONFLICT (name) DO UPDATE SET
			version = EXCLUDED.version,
			description = EXCLUDED.description,
			sub_timeout_ms = EXCLUDED.sub_timeout_ms,
			max_retries = EXCLUDED.max_retries,
			retry_backoff_ms = EXCLUDED.retry_backoff_ms,
			enable_monitor = EXCLUDED.enable_monitor,
			enable_idempotency = EXCLUDED.enable_idempotency,
			enable_poison = EXCLUDED.enable_poison,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`, p.tableName)

	var subTimeoutMs, retryBackoffMs *int64
	if schema.SubTimeout > 0 {
		ms := schema.SubTimeout.Milliseconds()
		subTimeoutMs = &ms
	}
	if schema.RetryBackoff > 0 {
		ms := schema.RetryBackoff.Milliseconds()
		retryBackoffMs = &ms
	}

	_, err = p.db.ExecContext(ctx, query,
		schema.Name,
		schema.Version,
		nullString(schema.Description),
		subTimeoutMs,
		nullInt(schema.MaxRetries),
		retryBackoffMs,
		schema.EnableMonitor,
		schema.EnableIdempotency,
		schema.EnablePoison,
		metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("set schema: %w", err)
	}

	// Notify watchers and publisher
	change := SchemaChangeEvent{
		EventName: schema.Name,
		Version:   schema.Version,
		UpdatedAt: time.Now(),
	}

	p.mu.RLock()
	for _, watcher := range p.watchers {
		select {
		case watcher <- change:
		default:
			// Watcher buffer full, skip
		}
	}
	p.mu.RUnlock()

	if p.publisher != nil {
		if err := p.publisher(ctx, change); err != nil {
			// Log but don't fail - schema is stored
			return nil
		}
	}

	return nil
}

// Delete removes a schema.
func (p *PostgresProvider) Delete(ctx context.Context, eventName string) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProviderClosed
	}
	p.mu.RUnlock()

	query := fmt.Sprintf(`DELETE FROM %s WHERE name = $1`, p.tableName)
	_, err := p.db.ExecContext(ctx, query, eventName)
	if err != nil {
		return fmt.Errorf("delete schema: %w", err)
	}
	return nil
}

// Watch returns a channel that receives schema change notifications.
func (p *PostgresProvider) Watch(ctx context.Context) (<-chan SchemaChangeEvent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrProviderClosed
	}

	ch := make(chan SchemaChangeEvent, 100)
	p.watchers = append(p.watchers, ch)

	// Close channel when context is done or provider is closed
	go func() {
		select {
		case <-ctx.Done():
		case <-p.closeChan:
		}
		p.mu.Lock()
		defer p.mu.Unlock()

		// Remove watcher
		for i, w := range p.watchers {
			if w == ch {
				p.watchers = append(p.watchers[:i], p.watchers[i+1:]...)
				break
			}
		}
		close(ch)
	}()

	return ch, nil
}

// List returns all schemas.
func (p *PostgresProvider) List(ctx context.Context) ([]*EventSchema, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrProviderClosed
	}
	p.mu.RUnlock()

	query := fmt.Sprintf(`
		SELECT name, version, description, sub_timeout_ms, max_retries, retry_backoff_ms,
		       enable_monitor, enable_idempotency, enable_poison, metadata,
		       created_at, updated_at
		FROM %s
		ORDER BY name
	`, p.tableName)

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}
	defer rows.Close()

	var schemas []*EventSchema
	for rows.Next() {
		schema, err := p.scanRows(rows)
		if err != nil {
			return nil, fmt.Errorf("scan schema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	return schemas, rows.Err()
}

// Close closes the provider.
func (p *PostgresProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.closeChan)
	return nil
}

// CreateTable creates the schema table if it doesn't exist.
// This is a convenience method for development and testing.
func (p *PostgresProvider) CreateTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name TEXT PRIMARY KEY,
			version INT NOT NULL DEFAULT 1,
			description TEXT,
			sub_timeout_ms BIGINT,
			max_retries INT,
			retry_backoff_ms BIGINT,
			enable_monitor BOOLEAN DEFAULT false,
			enable_idempotency BOOLEAN DEFAULT false,
			enable_poison BOOLEAN DEFAULT false,
			metadata JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_%s_updated ON %s(updated_at);
	`, p.tableName, p.tableName, p.tableName)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

func (p *PostgresProvider) scanRow(row *sql.Row) (*EventSchema, error) {
	var schema EventSchema
	var description sql.NullString
	var subTimeoutMs, retryBackoffMs sql.NullInt64
	var maxRetries sql.NullInt32
	var metadataJSON []byte

	err := row.Scan(
		&schema.Name,
		&schema.Version,
		&description,
		&subTimeoutMs,
		&maxRetries,
		&retryBackoffMs,
		&schema.EnableMonitor,
		&schema.EnableIdempotency,
		&schema.EnablePoison,
		&metadataJSON,
		&schema.CreatedAt,
		&schema.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if description.Valid {
		schema.Description = description.String
	}
	if subTimeoutMs.Valid {
		schema.SubTimeout = time.Duration(subTimeoutMs.Int64) * time.Millisecond
	}
	if maxRetries.Valid {
		schema.MaxRetries = int(maxRetries.Int32)
	}
	if retryBackoffMs.Valid {
		schema.RetryBackoff = time.Duration(retryBackoffMs.Int64) * time.Millisecond
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &schema.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &schema, nil
}

func (p *PostgresProvider) scanRows(rows *sql.Rows) (*EventSchema, error) {
	var schema EventSchema
	var description sql.NullString
	var subTimeoutMs, retryBackoffMs sql.NullInt64
	var maxRetries sql.NullInt32
	var metadataJSON []byte

	err := rows.Scan(
		&schema.Name,
		&schema.Version,
		&description,
		&subTimeoutMs,
		&maxRetries,
		&retryBackoffMs,
		&schema.EnableMonitor,
		&schema.EnableIdempotency,
		&schema.EnablePoison,
		&metadataJSON,
		&schema.CreatedAt,
		&schema.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if description.Valid {
		schema.Description = description.String
	}
	if subTimeoutMs.Valid {
		schema.SubTimeout = time.Duration(subTimeoutMs.Int64) * time.Millisecond
	}
	if maxRetries.Valid {
		schema.MaxRetries = int(maxRetries.Int32)
	}
	if retryBackoffMs.Valid {
		schema.RetryBackoff = time.Duration(retryBackoffMs.Int64) * time.Millisecond
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &schema.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &schema, nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullInt(n int) *int {
	if n == 0 {
		return nil
	}
	return &n
}

// Compile-time check that PostgresProvider implements SchemaProvider.
var _ SchemaProvider = (*PostgresProvider)(nil)
