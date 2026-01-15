package base

import (
	"database/sql"
	"testing"
	"time"
)

func TestQueryBuilder(t *testing.T) {
	t.Run("empty builder", func(t *testing.T) {
		qb := NewQueryBuilder()
		if qb.WhereClause() != "" {
			t.Error("expected empty where clause")
		}
		if qb.HasConditions() {
			t.Error("expected no conditions")
		}
	})

	t.Run("single condition", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.Add("name = $%d", "John")

		clause := qb.WhereClause()
		expected := "WHERE name = $1"
		if clause != expected {
			t.Errorf("expected %q, got %q", expected, clause)
		}

		args := qb.Args()
		if len(args) != 1 || args[0] != "John" {
			t.Errorf("expected [John], got %v", args)
		}
	})

	t.Run("multiple conditions", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.Add("name = $%d", "John")
		qb.Add("age > $%d", 18)

		clause := qb.WhereClause()
		expected := "WHERE name = $1 AND age > $2"
		if clause != expected {
			t.Errorf("expected %q, got %q", expected, clause)
		}
	})

	t.Run("AddIf", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.AddIf(true, "active = $%d", true)
		qb.AddIf(false, "deleted = $%d", true) // Should be skipped

		args := qb.Args()
		if len(args) != 1 {
			t.Errorf("expected 1 arg, got %d", len(args))
		}
	})

	t.Run("AddIfNotEmpty", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.AddIfNotEmpty("name = $%d", "John")
		qb.AddIfNotEmpty("email = $%d", "") // Should be skipped

		if len(qb.Args()) != 1 {
			t.Errorf("expected 1 arg, got %d", len(qb.Args()))
		}
	})

	t.Run("AddIfNotZero time", func(t *testing.T) {
		qb := NewQueryBuilder()
		now := time.Now()
		qb.AddIfNotZero("created_at >= $%d", now)
		qb.AddIfNotZero("deleted_at <= $%d", time.Time{}) // Should be skipped

		if len(qb.Args()) != 1 {
			t.Errorf("expected 1 arg, got %d", len(qb.Args()))
		}
	})

	t.Run("AddIfPositive", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.AddIfPositive("limit = $%d", 10)
		qb.AddIfPositive("offset = $%d", 0) // Should be skipped

		if len(qb.Args()) != 1 {
			t.Errorf("expected 1 arg, got %d", len(qb.Args()))
		}
	})

	t.Run("AddRaw", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.AddRaw("deleted_at IS NULL")
		qb.Add("status = $%d", "active")

		clause := qb.WhereClause()
		expected := "WHERE deleted_at IS NULL AND status = $1"
		if clause != expected {
			t.Errorf("expected %q, got %q", expected, clause)
		}
	})

	t.Run("AddIn", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.AddIn("status", []string{"active", "pending", "review"})

		clause := qb.WhereClause()
		expected := "WHERE status IN ($1, $2, $3)"
		if clause != expected {
			t.Errorf("expected %q, got %q", expected, clause)
		}

		args := qb.Args()
		if len(args) != 3 {
			t.Errorf("expected 3 args, got %d", len(args))
		}
	})

	t.Run("AddIn empty", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.AddIn("status", []string{})

		if qb.HasConditions() {
			t.Error("expected no conditions for empty IN")
		}
	})

	t.Run("Build", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.Add("status = $%d", "active")

		query, args := qb.Build("SELECT * FROM users %s ORDER BY id")
		expected := "SELECT * FROM users WHERE status = $1 ORDER BY id"
		if query != expected {
			t.Errorf("expected %q, got %q", expected, query)
		}
		if len(args) != 1 {
			t.Errorf("expected 1 arg, got %d", len(args))
		}
	})

	t.Run("AppendLimit", func(t *testing.T) {
		qb := NewQueryBuilder()
		qb.Add("status = $%d", "active")
		limitClause := qb.AppendLimit(10, 20)

		expected := " LIMIT $2 OFFSET $3"
		if limitClause != expected {
			t.Errorf("expected %q, got %q", expected, limitClause)
		}

		args := qb.Args()
		if len(args) != 3 {
			t.Errorf("expected 3 args, got %d", len(args))
		}
	})

	t.Run("NewQueryBuilderFrom", func(t *testing.T) {
		qb := NewQueryBuilderFrom(5)
		qb.Add("name = $%d", "John")

		clause := qb.WhereClause()
		expected := "WHERE name = $5"
		if clause != expected {
			t.Errorf("expected %q, got %q", expected, clause)
		}
	})
}

func TestNullHelpers(t *testing.T) {
	t.Run("NullString valid", func(t *testing.T) {
		ns := sql.NullString{String: "hello", Valid: true}
		if NullString(ns) != "hello" {
			t.Error("expected 'hello'")
		}
	})

	t.Run("NullString invalid", func(t *testing.T) {
		ns := sql.NullString{String: "hello", Valid: false}
		if NullString(ns) != "" {
			t.Error("expected empty string")
		}
	})

	t.Run("NullTime valid", func(t *testing.T) {
		now := time.Now()
		nt := sql.NullTime{Time: now, Valid: true}
		result := NullTime(nt)
		if result == nil || !result.Equal(now) {
			t.Error("expected time pointer")
		}
	})

	t.Run("NullTime invalid", func(t *testing.T) {
		nt := sql.NullTime{Valid: false}
		if NullTime(nt) != nil {
			t.Error("expected nil")
		}
	})

	t.Run("NullDurationMs", func(t *testing.T) {
		ni := sql.NullInt64{Int64: 1500, Valid: true}
		d := NullDurationMs(ni)
		if d != 1500*time.Millisecond {
			t.Errorf("expected 1500ms, got %v", d)
		}
	})

	t.Run("ToNullString", func(t *testing.T) {
		ns := ToNullString("hello")
		if !ns.Valid || ns.String != "hello" {
			t.Error("expected valid NullString")
		}

		nsEmpty := ToNullString("")
		if nsEmpty.Valid {
			t.Error("expected invalid NullString for empty")
		}
	})

	t.Run("StringPtr", func(t *testing.T) {
		p := StringPtr("hello")
		if p == nil || *p != "hello" {
			t.Error("expected pointer to 'hello'")
		}

		pEmpty := StringPtr("")
		if pEmpty != nil {
			t.Error("expected nil for empty string")
		}
	})
}
