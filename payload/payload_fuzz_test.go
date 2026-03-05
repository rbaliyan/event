package payload

import "testing"

func FuzzJSONCodecDecode(f *testing.F) {
	f.Add([]byte(`{"name":"test","value":42}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = JSON{}.Decode(data, &v)
	})
}

func FuzzMsgPackCodecDecode(f *testing.F) {
	f.Add([]byte{0x80})       // empty map
	f.Add([]byte{0x90})       // empty array
	f.Add([]byte{0xc0})       // nil
	f.Add([]byte{0xc3})       // true
	f.Add([]byte{0xa5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}) // "hello"
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = MsgPack{}.Decode(data, &v)
	})
}
