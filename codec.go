package event

import (
	"bytes"
	"encoding/gob"
)

/*
TODO: fix encoding
*/

func init() {
	gob.Register(&RemoteMsg{})
}

type RemoteMsg struct {
	Data Data
}

// Unmarshal parses the encoded data and stores the result
// in the value pointed to by v
func Unmarshal(data []byte) (interface{}, error) {
	var got RemoteMsg
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&got); err != nil {
		return nil, err
	}
	return got.Data, nil
}

// Marshal encodes v and returns encoded data
func Marshal(v interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(&RemoteMsg{Data: v})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
