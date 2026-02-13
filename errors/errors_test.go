package errors

import (
	"database/sql"
	"errors"
	"testing"
)

func TestRequireNotNil(t *testing.T) {
	t.Run("untyped nil", func(t *testing.T) {
		err := RequireNotNil(nil, "db")
		if err == nil {
			t.Fatal("expected error for untyped nil")
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	t.Run("typed nil pointer", func(t *testing.T) {
		var db *sql.DB
		err := RequireNotNil(db, "db")
		if err == nil {
			t.Fatal("expected error for typed nil pointer")
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	t.Run("typed nil map", func(t *testing.T) {
		var m map[string]string
		err := RequireNotNil(m, "config")
		if err == nil {
			t.Fatal("expected error for typed nil map")
		}
	})

	t.Run("typed nil slice", func(t *testing.T) {
		var s []int
		err := RequireNotNil(s, "items")
		if err == nil {
			t.Fatal("expected error for typed nil slice")
		}
	})

	t.Run("typed nil func", func(t *testing.T) {
		var fn func()
		err := RequireNotNil(fn, "callback")
		if err == nil {
			t.Fatal("expected error for typed nil func")
		}
	})

	t.Run("non-nil pointer", func(t *testing.T) {
		db := &sql.DB{}
		err := RequireNotNil(db, "db")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("non-nil value", func(t *testing.T) {
		err := RequireNotNil(42, "count")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error message includes name", func(t *testing.T) {
		err := RequireNotNil(nil, "transport")
		if err == nil {
			t.Fatal("expected error")
		}
		if got := err.Error(); got != "transport must not be nil: invalid argument" {
			t.Errorf("unexpected message: %s", got)
		}
	})
}

func TestRequireNotEmpty(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		err := RequireNotEmpty("", "name")
		if err == nil {
			t.Fatal("expected error for empty string")
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	t.Run("non-empty string", func(t *testing.T) {
		err := RequireNotEmpty("hello", "name")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error message includes name", func(t *testing.T) {
		err := RequireNotEmpty("", "event_name")
		if err == nil {
			t.Fatal("expected error")
		}
		if got := err.Error(); got != "event_name must not be empty: invalid argument" {
			t.Errorf("unexpected message: %s", got)
		}
	})
}

func TestRequirePositive(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		err := RequirePositive(0, "count")
		if err == nil {
			t.Fatal("expected error for zero")
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	t.Run("negative", func(t *testing.T) {
		err := RequirePositive(-1, "count")
		if err == nil {
			t.Fatal("expected error for negative")
		}
	})

	t.Run("positive", func(t *testing.T) {
		err := RequirePositive(1, "count")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error message includes value", func(t *testing.T) {
		err := RequirePositive(-5, "retries")
		if err == nil {
			t.Fatal("expected error")
		}
		if got := err.Error(); got != "retries must be positive, got -5: invalid argument" {
			t.Errorf("unexpected message: %s", got)
		}
	})
}

func TestRequireNonNegative(t *testing.T) {
	t.Run("negative", func(t *testing.T) {
		err := RequireNonNegative(-1, "offset")
		if err == nil {
			t.Fatal("expected error for negative")
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	t.Run("zero", func(t *testing.T) {
		err := RequireNonNegative(0, "offset")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("positive", func(t *testing.T) {
		err := RequireNonNegative(5, "offset")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error message includes value", func(t *testing.T) {
		err := RequireNonNegative(-3, "timeout")
		if err == nil {
			t.Fatal("expected error")
		}
		if got := err.Error(); got != "timeout must be non-negative, got -3: invalid argument" {
			t.Errorf("unexpected message: %s", got)
		}
	})
}
