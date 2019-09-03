package commander

import (
	"encoding/json"

	"github.com/jeroenrinzema/commander/types"
)

// WithJSONCodec constructs a new JSON message interperter used for encoding and decoding messages.
func WithJSONCodec() types.GroupOption {
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
func (codec *JSONCodec) Apply(options *types.GroupOptions) {
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
