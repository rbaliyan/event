package codec

import "testing"

func FuzzJSONTransportCodecDecode(f *testing.F) {
	f.Add([]byte(`{"id":"abc","source":"test","payload":"aGVsbG8=","metadata":{"key":"val"}}`))
	f.Add([]byte(`{"id":"","source":"","payload":""}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`{"id":"x","source":"s","payload":"dGVzdA==","retry_count":3}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = JSON{}.Decode(data)
	})
}

func FuzzMsgPackTransportCodecDecode(f *testing.F) {
	f.Add([]byte{0x80})
	f.Add([]byte{0x85, 0xa2, 0x69, 0x64, 0xa1, 0x78})
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff})
	f.Add([]byte{0xc0})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = MsgPack{}.Decode(data)
	})
}
