package types

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
