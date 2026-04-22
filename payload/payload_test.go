package payload

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Test structs for JSON, MsgPack, and BSON
type TestOrder struct {
	ID       string  `json:"id" msgpack:"id" bson:"_id"`
	Product  string  `json:"product" msgpack:"product" bson:"product"`
	Quantity int     `json:"quantity" msgpack:"quantity" bson:"quantity"`
	Price    float64 `json:"price" msgpack:"price" bson:"price"`
}

// TestJSON tests JSON codec
func TestJSON(t *testing.T) {
	codec := JSON{}

	t.Run("ContentType", func(t *testing.T) {
		if got := codec.ContentType(); got != "application/json" {
			t.Errorf("ContentType() = %v, want application/json", got)
		}
	})

	t.Run("Encode", func(t *testing.T) {
		order := TestOrder{
			ID:       "order-123",
			Product:  "Widget",
			Quantity: 5,
			Price:    99.99,
		}

		data, err := codec.Encode(order)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if len(data) == 0 {
			t.Error("Encode() returned empty data")
		}
	})

	t.Run("Decode", func(t *testing.T) {
		original := TestOrder{
			ID:       "order-456",
			Product:  "Gadget",
			Quantity: 10,
			Price:    49.50,
		}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		var decoded TestOrder
		if err := codec.Decode(data, &decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if decoded != original {
			t.Errorf("Decode() = %+v, want %+v", decoded, original)
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		original := TestOrder{
			ID:       "order-789",
			Product:  "Doohickey",
			Quantity: 3,
			Price:    199.99,
		}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		var decoded TestOrder
		if err := codec.Decode(data, &decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if decoded.ID != original.ID || decoded.Product != original.Product ||
			decoded.Quantity != original.Quantity || decoded.Price != original.Price {
			t.Errorf("RoundTrip failed: got %+v, want %+v", decoded, original)
		}
	})
}

// TestMsgPack tests MessagePack codec
func TestMsgPack(t *testing.T) {
	codec := MsgPack{}

	t.Run("ContentType", func(t *testing.T) {
		if got := codec.ContentType(); got != "application/msgpack" {
			t.Errorf("ContentType() = %v, want application/msgpack", got)
		}
	})

	t.Run("Encode", func(t *testing.T) {
		order := TestOrder{
			ID:       "order-msgpack-1",
			Product:  "MsgPack Widget",
			Quantity: 7,
			Price:    79.99,
		}

		data, err := codec.Encode(order)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if len(data) == 0 {
			t.Error("Encode() returned empty data")
		}
	})

	t.Run("Decode", func(t *testing.T) {
		original := TestOrder{
			ID:       "order-msgpack-2",
			Product:  "MsgPack Gadget",
			Quantity: 15,
			Price:    29.99,
		}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		var decoded TestOrder
		if err := codec.Decode(data, &decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if decoded != original {
			t.Errorf("Decode() = %+v, want %+v", decoded, original)
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		original := TestOrder{
			ID:       "order-msgpack-3",
			Product:  "Binary Widget",
			Quantity: 20,
			Price:    149.99,
		}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		var decoded TestOrder
		if err := codec.Decode(data, &decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if decoded != original {
			t.Errorf("RoundTrip failed: got %+v, want %+v", decoded, original)
		}
	})
}

// TestProto tests Protocol Buffers codec
func TestProto(t *testing.T) {
	codec := Proto{}

	t.Run("ContentType", func(t *testing.T) {
		if got := codec.ContentType(); got != "application/protobuf" {
			t.Errorf("ContentType() = %v, want application/protobuf", got)
		}
	})

	t.Run("Encode_Success", func(t *testing.T) {
		msg := &timestamppb.Timestamp{
			Seconds: 1234567890,
			Nanos:   123456789,
		}

		data, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if len(data) == 0 {
			t.Error("Encode() returned empty data")
		}
	})

	t.Run("Encode_NonProtoMessage", func(t *testing.T) {
		notProto := TestOrder{ID: "test"}

		_, err := codec.Encode(notProto)
		if err == nil {
			t.Error("Encode() should error for non-proto.Message type")
		}
	})

	t.Run("Decode_Success", func(t *testing.T) {
		original := &timestamppb.Timestamp{
			Seconds: 9876543210,
			Nanos:   987654321,
		}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded := &timestamppb.Timestamp{}
		if err := codec.Decode(data, decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if !proto.Equal(original, decoded) {
			t.Errorf("Decode() = %+v, want %+v", decoded, original)
		}
	})

	t.Run("Decode_NonProtoMessage", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03}
		var notProto TestOrder

		err := codec.Decode(data, &notProto)
		if err == nil {
			t.Error("Decode() should error for non-proto.Message type")
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		original := &timestamppb.Timestamp{
			Seconds: 1111111111,
			Nanos:   222222222,
		}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded := &timestamppb.Timestamp{}
		if err := codec.Decode(data, decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if original.Seconds != decoded.Seconds || original.Nanos != decoded.Nanos {
			t.Errorf("RoundTrip failed: got %+v, want %+v", decoded, original)
		}
	})
}

// TestText tests Text codec
func TestText(t *testing.T) {
	codec := Text{}

	t.Run("ContentType", func(t *testing.T) {
		if got := codec.ContentType(); got != "text/plain" {
			t.Errorf("ContentType() = %v, want text/plain", got)
		}
	})

	t.Run("Encode_String", func(t *testing.T) {
		text := "hello world"

		data, err := codec.Encode(text)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if string(data) != text {
			t.Errorf("Encode() = %s, want %s", string(data), text)
		}
	})

	t.Run("Encode_StringPointer", func(t *testing.T) {
		text := "pointer string"

		data, err := codec.Encode(&text)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if string(data) != text {
			t.Errorf("Encode() = %s, want %s", string(data), text)
		}
	})

	t.Run("Encode_NilPointer", func(t *testing.T) {
		var text *string

		data, err := codec.Encode(text)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if data != nil {
			t.Errorf("Encode() = %v, want nil", data)
		}
	})

	t.Run("Encode_Bytes", func(t *testing.T) {
		text := []byte("byte slice")

		data, err := codec.Encode(text)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
		if string(data) != string(text) {
			t.Errorf("Encode() = %s, want %s", string(data), string(text))
		}
	})

	t.Run("Encode_InvalidType", func(t *testing.T) {
		_, err := codec.Encode(123)
		if err == nil {
			t.Error("Encode() should error for non-string type")
		}
	})

	t.Run("Decode_Success", func(t *testing.T) {
		original := "decoded text"
		data := []byte(original)

		var decoded string
		if err := codec.Decode(data, &decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if decoded != original {
			t.Errorf("Decode() = %s, want %s", decoded, original)
		}
	})

	t.Run("Decode_InvalidType", func(t *testing.T) {
		data := []byte("test")
		var notString int

		err := codec.Decode(data, &notString)
		if err == nil {
			t.Error("Decode() should error for non-string pointer type")
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		original := "round trip text"

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		var decoded string
		if err := codec.Decode(data, &decoded); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if decoded != original {
			t.Errorf("RoundTrip failed: got %s, want %s", decoded, original)
		}
	})
}

// TestDefault tests the Default function
func TestDefault(t *testing.T) {
	codec := Default()
	if codec == nil {
		t.Fatal("Default() returned nil")
	}

	// Default should be JSON
	if codec.ContentType() != "application/json" {
		t.Errorf("Default() ContentType = %v, want application/json", codec.ContentType())
	}

	// Verify it works as JSON codec
	order := TestOrder{
		ID:       "default-test",
		Product:  "Default Widget",
		Quantity: 1,
		Price:    9.99,
	}

	data, err := codec.Encode(order)
	if err != nil {
		t.Fatalf("Default codec Encode() error = %v", err)
	}

	var decoded TestOrder
	if err := codec.Decode(data, &decoded); err != nil {
		t.Fatalf("Default codec Decode() error = %v", err)
	}

	if decoded != order {
		t.Errorf("Default codec failed: got %+v, want %+v", decoded, order)
	}
}

// TestRegistry tests the registry functions
func TestRegistry(t *testing.T) {
	t.Run("Register_and_Get", func(t *testing.T) {
		// JSON should be registered by default
		codec, ok := Get("application/json")
		if !ok {
			t.Error("Get(application/json) should return true")
		}
		if codec == nil {
			t.Error("Get(application/json) returned nil codec")
		}
		if codec.ContentType() != "application/json" {
			t.Errorf("codec.ContentType() = %v, want application/json", codec.ContentType())
		}
	})

	t.Run("Get_NotFound", func(t *testing.T) {
		codec, ok := Get("application/unknown")
		if ok {
			t.Error("Get(application/unknown) should return false")
		}
		if codec != nil {
			t.Error("Get(application/unknown) should return nil codec")
		}
	})

	t.Run("MustGet_Found", func(t *testing.T) {
		codec := MustGet("application/json")
		if codec == nil {
			t.Fatal("MustGet(application/json) returned nil")
		}
		if codec.ContentType() != "application/json" {
			t.Errorf("codec.ContentType() = %v, want application/json", codec.ContentType())
		}
	})

	t.Run("MustGet_NotFound_ReturnsDefault", func(t *testing.T) {
		codec := MustGet("application/unknown")
		if codec == nil {
			t.Fatal("MustGet(application/unknown) returned nil")
		}
		// Should return JSON as default
		if codec.ContentType() != "application/json" {
			t.Errorf("MustGet fallback ContentType() = %v, want application/json", codec.ContentType())
		}
	})

	t.Run("Register_MsgPack", func(t *testing.T) {
		// MsgPack should be registered by init
		codec, ok := Get("application/msgpack")
		if !ok {
			t.Error("Get(application/msgpack) should return true (registered in init)")
		}
		if codec == nil {
			t.Error("Get(application/msgpack) returned nil")
		}
	})

	t.Run("Register_Proto", func(t *testing.T) {
		// Proto should be registered by init
		codec, ok := Get("application/protobuf")
		if !ok {
			t.Error("Get(application/protobuf) should return true (registered in init)")
		}
		if codec == nil {
			t.Error("Get(application/protobuf) returned nil")
		}
	})

	t.Run("Register_Text", func(t *testing.T) {
		// Text should be registered by init
		codec, ok := Get("text/plain")
		if !ok {
			t.Error("Get(text/plain) should return true (registered in init)")
		}
		if codec == nil {
			t.Error("Get(text/plain) returned nil")
		}
	})
}

// TestCodecInterface verifies all codecs implement the Codec interface
func TestCodecInterface(t *testing.T) {
	codecs := []Codec{
		JSON{},
		MsgPack{},
		Proto{},
		Text{},
	}

	for _, codec := range codecs {
		t.Run(codec.ContentType(), func(t *testing.T) {
			// Verify ContentType returns non-empty string
			if codec.ContentType() == "" {
				t.Error("ContentType() returned empty string")
			}

			// All codecs should have Encode and Decode methods
			// (verified by compilation, but we can do a runtime check)
			var _ Codec = codec
		})
	}
}

// BenchmarkJSON benchmarks JSON codec
func BenchmarkJSON(b *testing.B) {
	codec := JSON{}
	order := TestOrder{
		ID:       "bench-order-123",
		Product:  "Benchmark Widget",
		Quantity: 100,
		Price:    999.99,
	}

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := codec.Encode(order)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	data, _ := codec.Encode(order)
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var decoded TestOrder
			err := codec.Decode(data, &decoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMsgPack benchmarks MessagePack codec
func BenchmarkMsgPack(b *testing.B) {
	codec := MsgPack{}
	order := TestOrder{
		ID:       "bench-order-msgpack",
		Product:  "Benchmark MsgPack Widget",
		Quantity: 100,
		Price:    999.99,
	}

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := codec.Encode(order)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	data, _ := codec.Encode(order)
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var decoded TestOrder
			err := codec.Decode(data, &decoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkText benchmarks Text codec
func BenchmarkText(b *testing.B) {
	codec := Text{}
	text := "benchmark text string for performance testing"

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := codec.Encode(text)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	data, _ := codec.Encode(text)
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var decoded string
			err := codec.Decode(data, &decoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
