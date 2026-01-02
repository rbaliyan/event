package saga

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

/*
PostgreSQL Schema:

CREATE TABLE sagas (
    id              VARCHAR(36) PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    status          VARCHAR(50) NOT NULL,
    current_step    INT NOT NULL DEFAULT 0,
    completed_steps TEXT[],
    data            JSONB,
    error           TEXT,
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP,
    last_updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_sagas_name ON sagas(name);
CREATE INDEX idx_sagas_status ON sagas(status);
CREATE INDEX idx_sagas_started_at ON sagas(started_at);
*/

// PostgresStore is a PostgreSQL-based saga store
type PostgresStore struct {
	db    *sql.DB
	table string
}

// NewPostgresStore creates a new PostgreSQL saga store
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{
		db:    db,
		table: "sagas",
	}
}

// WithTable sets a custom table name
func (s *PostgresStore) WithTable(table string) *PostgresStore {
	s.table = table
	return s
}

// Create creates a new saga instance
func (s *PostgresStore) Create(ctx context.Context, state *State) error {
	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, name, status, current_step, completed_steps, data, error, started_at, completed_at, last_updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		state.ID,
		state.Name,
		state.Status,
		state.CurrentStep,
		state.CompletedSteps,
		data,
		state.Error,
		state.StartedAt,
		state.CompletedAt,
		state.LastUpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves saga state by ID
func (s *PostgresStore) Get(ctx context.Context, id string) (*State, error) {
	query := fmt.Sprintf(`
		SELECT id, name, status, current_step, completed_steps, data, error, started_at, completed_at, last_updated_at
		FROM %s
		WHERE id = $1
	`, s.table)

	var state State
	var data []byte
	var completedSteps []string
	var completedAt sql.NullTime
	var errorStr sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&state.ID,
		&state.Name,
		&state.Status,
		&state.CurrentStep,
		&completedSteps,
		&data,
		&errorStr,
		&state.StartedAt,
		&completedAt,
		&state.LastUpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("saga not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	state.CompletedSteps = completedSteps

	if len(data) > 0 {
		if err := json.Unmarshal(data, &state.Data); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
	}

	if completedAt.Valid {
		state.CompletedAt = &completedAt.Time
	}

	if errorStr.Valid {
		state.Error = errorStr.String
	}

	return &state, nil
}

// Update updates saga state
func (s *PostgresStore) Update(ctx context.Context, state *State) error {
	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET status = $1, current_step = $2, completed_steps = $3, data = $4, error = $5, completed_at = $6, last_updated_at = $7
		WHERE id = $8
	`, s.table)

	result, err := s.db.ExecContext(ctx, query,
		state.Status,
		state.CurrentStep,
		state.CompletedSteps,
		data,
		state.Error,
		state.CompletedAt,
		state.LastUpdatedAt,
		state.ID,
	)

	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("saga not found: %s", state.ID)
	}

	return nil
}

// List lists sagas matching the filter
func (s *PostgresStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	query := fmt.Sprintf(`
		SELECT id, name, status, current_step, completed_steps, data, error, started_at, completed_at, last_updated_at
		FROM %s
		WHERE 1=1
	`, s.table)

	var args []interface{}
	argIndex := 1

	if filter.Name != "" {
		query += fmt.Sprintf(" AND name = $%d", argIndex)
		args = append(args, filter.Name)
		argIndex++
	}

	if len(filter.Status) > 0 {
		placeholders := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, status)
			argIndex++
		}
		query += fmt.Sprintf(" AND status IN (%s)", strings.Join(placeholders, ", "))
	}

	query += " ORDER BY started_at DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var results []*State
	for rows.Next() {
		var state State
		var data []byte
		var completedSteps []string
		var completedAt sql.NullTime
		var errorStr sql.NullString

		err := rows.Scan(
			&state.ID,
			&state.Name,
			&state.Status,
			&state.CurrentStep,
			&completedSteps,
			&data,
			&errorStr,
			&state.StartedAt,
			&completedAt,
			&state.LastUpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		state.CompletedSteps = completedSteps

		if len(data) > 0 {
			if err := json.Unmarshal(data, &state.Data); err != nil {
				return nil, fmt.Errorf("unmarshal data: %w", err)
			}
		}

		if completedAt.Valid {
			state.CompletedAt = &completedAt.Time
		}

		if errorStr.Valid {
			state.Error = errorStr.String
		}

		results = append(results, &state)
	}

	return results, nil
}

// DeleteOlderThan removes sagas older than the specified age
func (s *PostgresStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE started_at < $1", s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now().Add(-age))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// Compile-time check
var _ Store = (*PostgresStore)(nil)
