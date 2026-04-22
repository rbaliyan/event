package base

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// SQLNotFound translates sql.ErrNoRows to fmt.Errorf("%s: %w", id, sentinel).
// Returns err unchanged for any other error value, including nil.
//
// Typical usage in a Get method:
//
//	err := row.Scan(...)
//	if err = base.SQLNotFound(err, id, ErrNotFound); err != nil {
//	    return nil, err
//	}
func SQLNotFound(err error, id string, sentinel error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", id, sentinel)
	}
	return err
}

// RedisNotFound translates redis.Nil to fmt.Errorf("%s: %w", id, sentinel).
// Returns err unchanged for any other error value, including nil.
//
// Typical usage in a Get method:
//
//	data, err := client.HGet(ctx, key, id).Result()
//	if err = base.RedisNotFound(err, id, ErrNotFound); err != nil {
//	    return nil, err
//	}
func RedisNotFound(err error, id string, sentinel error) error {
	if errors.Is(err, redis.Nil) {
		return fmt.Errorf("%s: %w", id, sentinel)
	}
	return err
}
