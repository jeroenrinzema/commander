package io

import (
	"bytes"
	"encoding/gob"

	"github.com/jeroenrinzema/commander/internal/types"
)

// Marshaler encoding and decoding implementation.
type Marshaler interface {
	Unmarshal([]byte) error
	Marshal(interface{}) ([]byte, error)
}

// Gob marshaler implementation using the encoding/gob package
type Gob struct{}

// Unmarshal attempts to decode the given bytes into a types.Message
func (_ Gob) Unmarshal(chunk []byte) (*types.Message, error) {
	buf := bytes.NewBuffer(chunk)
	decoder := gob.NewDecoder(buf)

	var msg = new(types.Message)
	err := decoder.Decode(msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Marshal attempts to encode the given message into a slice of bytes
func (_ Gob) Marshal(msg *types.Message) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)

	err := encoder.Encode(msg)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
