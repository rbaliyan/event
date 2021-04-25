package event

import (
	"bytes"
	"encoding/gob"
)

/*
TODO: fix encoding
*/

type rpcmsg struct {
	ID   string
	Data Data
}

// Unmarshal parses the encoded data and stores the result
// in the value pointed to by v
func Unmarshal(data []byte) (interface{}, error) {
	var got rpcmsg
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
	err := enc.Encode(&rpcmsg{Data: v})
	if err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil
}
