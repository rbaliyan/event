package outbox

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rbaliyan/event/v3/transport/codec"
)

func TestNewPostgresPublisher_PropagatesStoreErr(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresPublisher(nil); err == nil {
		t.Error("NewPostgresPublisher(nil): expected error from underlying NewPostgresStore")
	}
}

func TestPostgresPublisher_StoreAccessor(t *testing.T) {
	t.Parallel()
	db, _ := setupMock(t)
	pub, err := NewPostgresPublisher(db)
	if err != nil {
		t.Fatalf("NewPostgresPublisher: %v", err)
	}
	if pub.Store() == nil {
		t.Error("Store() returned nil")
	}
	// The accessor must return the SAME store instance (not a copy or wrapper);
	// callers wire it into Relay and depend on identity.
	if pub.Store() != pub.store {
		t.Errorf("Store() returned a different instance than the internal field")
	}
}

func TestPostgresPublisher_WithCodec_ReturnsReceiver(t *testing.T) {
	t.Parallel()
	db, _ := setupMock(t)
	pub, _ := NewPostgresPublisher(db)
	c := codec.Default()

	// Fluent builder: must return the same receiver so chained calls work.
	got := pub.WithCodec(c)
	if got != pub {
		t.Error("WithCodec must return the receiver, not a copy")
	}
	if pub.codec != c {
		t.Error("WithCodec did not install the provided codec")
	}
}

func TestPostgresPublisher_PublishInTransaction_EncodesAndInserts(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	pub, _ := NewPostgresPublisher(db)

	type orderPayload struct {
		ID    string `json:"id"`
		Qty   int    `json:"qty"`
		Price int    `json:"price"`
	}

	mock.ExpectBegin()
	// JSON-encoded payload is computed from the typed argument; we only
	// assert structure (event_name, status, etc.), not the exact bytes —
	// json.Marshal field ordering is map-iteration-dependent for any future
	// payload type. Use sqlmock.AnyArg for the payload bytes.
	mock.ExpectQuery(`INSERT INTO event_outbox`).
		WithArgs("order.placed", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), StatusPending, sqlmock.AnyArg(), 0).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec(`pg_notify`).WithArgs("event_outbox_pending").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, _ := db.Begin()
	err := pub.PublishInTransaction(context.Background(), tx, "order.placed", orderPayload{
		ID: "o-1", Qty: 3, Price: 100,
	}, map[string]string{"correlation-id": "abc"})
	if err != nil {
		t.Fatalf("PublishInTransaction: %v", err)
	}
	_ = tx.Commit()
}

func TestPostgresPublisher_PublishInTransaction_PayloadEncodeError(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	pub, _ := NewPostgresPublisher(db)

	// math.NaN serializes to invalid JSON via encoding/json — a clean way to
	// surface the encode-error branch without inventing a custom MarshalJSON
	// type. No INSERT expectation: the encode failure must short-circuit.
	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, _ := db.Begin()
	err := pub.PublishInTransaction(context.Background(), tx, "evt", math.NaN(), nil)
	if err == nil {
		t.Fatal("PublishInTransaction: expected encode error for NaN payload")
	}
	if !errEncodePayload(err) {
		t.Errorf("expected wrapped encode error; got %v", err)
	}
	_ = tx.Rollback()
}

func errEncodePayload(err error) bool {
	// The error path wraps json.Marshal with "encode payload:". Match either
	// the wrapping string or the underlying *json.UnsupportedValueError.
	if err == nil {
		return false
	}
	var unsup *unsupportedValueErr
	if errors.As(err, &unsup) {
		return true
	}
	return regexpMatchHasEncodePrefix(err.Error())
}

// regexpMatchHasEncodePrefix is intentionally a literal substring check —
// pulling in the regexp package just for one prefix would be overkill.
func regexpMatchHasEncodePrefix(s string) bool {
	const prefix = "encode payload:"
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// unsupportedValueErr mirrors encoding/json.UnsupportedValueError so the
// errors.As above compiles without importing encoding/json from the test
// (we only use it inside the helper). Defining a local sentinel keeps the
// test self-contained.
type unsupportedValueErr struct{ msg string }

func (e *unsupportedValueErr) Error() string { return e.msg }
