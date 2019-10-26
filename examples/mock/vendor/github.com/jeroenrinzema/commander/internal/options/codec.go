package options

import (
	"encoding/json"
)

// Codec Codec defines the interface commander uses to encode and decode messages.
// Note that implementations of this interface must be thread safe; a Codec's methods can be called from concurrent goroutines.
type Codec interface {
	// Marshal returns the wire format of s.
	// The s interface could be nil which represents a unkown schema format.
	// When no schema is defined should a default schema be used or a error be thrown.
	Marshal(s interface{}) ([]byte, error)
	// Unmarshal parses the wire format into s.
	// The s interface could be nil which represents a unkown schema format.
	// When no schema is defined should a default schema be used or a error be thrown.
	Unmarshal(data []byte, s interface{}) error
	// Schema returns the default schema implementation for the given codec.
	Schema() interface{}
}

// DefaultCodec returns the default codec that preforms no action during marshalling ur unmarshalling
func DefaultCodec() Codec {
	return &IgnoreCodec{}
}

// IgnoreCodec is the default codec interperter that preforms no action during marshalling ur unmarshalling
type IgnoreCodec struct {
}

// Schema returns the default schema implementation for the JSON codec
func (codec *IgnoreCodec) Schema() interface{} {
	return make([]byte, 0)
}

// Apply applies the given JSON codec to the given server options
func (codec *IgnoreCodec) Apply(options *GroupOptions) {
	options.Codec = codec
}

// Marshal returns the wire format of s.
func (codec *IgnoreCodec) Marshal(s interface{}) (bb []byte, err error) {
	return nil, nil
}

// Unmarshal parses the wire format into s.
func (codec *IgnoreCodec) Unmarshal(data []byte, s interface{}) (err error) {
	return nil
}

// WithJSONCodec constructs a new JSON message interperter used for encoding and decoding messages.
func WithJSONCodec() GroupOption {
	return &JSONCodec{}
}

// JSONCodec is a JSON message codec interperter used for encoding and decoding messages.
// All messages are decoded as map[string]interface{}.
type JSONCodec struct {
}

// Schema returns the default schema implementation for the JSON codec
func (codec *JSONCodec) Schema() interface{} {
	return make(map[string]interface{})
}

// Apply applies the given JSON codec to the given server options
func (codec *JSONCodec) Apply(options *GroupOptions) {
	options.Codec = codec
}

// Marshal returns the wire format of s.
func (codec *JSONCodec) Marshal(s interface{}) (bb []byte, err error) {
	bb, err = json.Marshal(s)
	return bb, err
}

// Unmarshal parses the wire format into s.
func (codec *JSONCodec) Unmarshal(data []byte, s interface{}) (err error) {
	err = json.Unmarshal(data, &s)
	return err
}
