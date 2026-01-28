package codec

import (
	"errors"
	"time"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/bson"
)

// BSON implements the Codec interface using BSON encoding.
// This is the preferred codec for MongoDB-based transports.
type BSON struct{}

// bsonEnvelope is the BSON wire format for messages
type bsonEnvelope struct {
	ID         string            `bson:"id"`
	Source     string            `bson:"source"`
	Payload    bson.Raw          `bson:"payload"`
	Metadata   map[string]string `bson:"metadata,omitempty"`
	Timestamp  time.Time         `bson:"timestamp"`
	RetryCount int               `bson:"retry_count,omitempty"`
}

// Encode serializes a message to BSON bytes.
func (c BSON) Encode(msg Message) ([]byte, error) {
	env := bsonEnvelope{
		ID:         msg.ID(),
		Source:     msg.Source(),
		Payload:    msg.Payload(),
		Metadata:   msg.Metadata(),
		Timestamp:  msg.Timestamp(),
		RetryCount: msg.RetryCount(),
	}

	data, err := bson.Marshal(env)
	if err != nil {
		return nil, errors.Join(ErrEncodeFailure, err)
	}

	return data, nil
}

// Decode deserializes BSON bytes to a message.
func (c BSON) Decode(data []byte) (Message, error) {
	var env bsonEnvelope
	if err := bson.Unmarshal(data, &env); err != nil {
		return nil, errors.Join(ErrDecodeFailure, err)
	}

	return message.New(
		env.ID,
		env.Source,
		env.Payload,
		env.Metadata,
		message.WithTimestamp(env.Timestamp),
		message.WithRetryCount(env.RetryCount),
	), nil
}

// ContentType returns the MIME type for BSON.
func (c BSON) ContentType() string {
	return "application/bson"
}

// Name returns the codec identifier.
func (c BSON) Name() string {
	return "bson"
}

// Compile-time interface check
var _ Codec = BSON{}
