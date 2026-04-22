package base_test

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/rbaliyan/event/v3/store/base"
)

var errSentinel = errors.New("not found")

func TestSQLNotFound(t *testing.T) {
	t.Run("translates ErrNoRows", func(t *testing.T) {
		err := base.SQLNotFound(sql.ErrNoRows, "id-1", errSentinel)
		if !errors.Is(err, errSentinel) {
			t.Fatalf("expected sentinel, got %v", err)
		}
		if err.Error() != "id-1: not found" {
			t.Fatalf("unexpected message: %s", err.Error())
		}
	})

	t.Run("passes other errors through", func(t *testing.T) {
		other := errors.New("connection reset")
		err := base.SQLNotFound(other, "id-1", errSentinel)
		if err != other {
			t.Fatalf("expected original error, got %v", err)
		}
	})

	t.Run("passes nil through", func(t *testing.T) {
		if err := base.SQLNotFound(nil, "id-1", errSentinel); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("wrapped ErrNoRows is translated", func(t *testing.T) {
		wrapped := errors.Join(sql.ErrNoRows, errors.New("detail"))
		err := base.SQLNotFound(wrapped, "id-2", errSentinel)
		if !errors.Is(err, errSentinel) {
			t.Fatalf("expected sentinel for wrapped ErrNoRows, got %v", err)
		}
	})
}

func TestRedisNotFound(t *testing.T) {
	t.Run("translates redis.Nil", func(t *testing.T) {
		err := base.RedisNotFound(redis.Nil, "id-1", errSentinel)
		if !errors.Is(err, errSentinel) {
			t.Fatalf("expected sentinel, got %v", err)
		}
		if err.Error() != "id-1: not found" {
			t.Fatalf("unexpected message: %s", err.Error())
		}
	})

	t.Run("passes other errors through", func(t *testing.T) {
		other := errors.New("connection reset")
		err := base.RedisNotFound(other, "id-1", errSentinel)
		if err != other {
			t.Fatalf("expected original error, got %v", err)
		}
	})

	t.Run("passes nil through", func(t *testing.T) {
		if err := base.RedisNotFound(nil, "id-1", errSentinel); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})
}
