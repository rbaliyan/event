package schema

import (
	"context"
	"testing"
)

func FuzzDecodeEnvelope(f *testing.F) {
	f.Add([]byte(`{"version":1,"event":"orders.created","payload":{"id":"123"}}`))
	f.Add([]byte(`{"version":2,"event":"test","payload":null,"metadata":{"key":"val"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`{"version":-1,"event":"","payload":""}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeEnvelope(data)
	})
}

func FuzzJSONSchemaValidate(f *testing.F) {
	f.Add([]byte(`{"order_id":"123","total":99.99}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"order_id":123}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`[]`))

	schema := NewJSONSchema("test", 1).
		WithRequired("order_id").
		WithProperty("order_id", "string").
		WithProperty("total", "number")

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = schema.Validate(data)
	})
}

func FuzzFieldMapperUpcast(f *testing.F) {
	f.Add([]byte(`{"customer_name":"John","legacy_id":"old123","amount":50}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"customer_name":"Jane"}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`{"email":"already@exists.com","customer_name":"Bob"}`))

	mapper := NewFieldMapper(1, 2).
		RenameField("customer_name", "customerName").
		AddDefault("email", "unknown@example.com").
		RemoveField("legacy_id")

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = mapper.Upcast(context.Background(), data)
	})
}
