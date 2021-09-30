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
	ID       string
	Source   string
	Data     Data
	Metadata []byte
}

// Unmarshal parses the encoded data and stores the result
// in the value pointed to by v
func Unmarshal(data []byte) (*RemoteMsg, error) {
	var got RemoteMsg
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&got); err != nil {
		return nil, err
	}
	return &got, nil
}

// Marshal encodes v and returns encoded data
func Marshal(v *RemoteMsg) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
