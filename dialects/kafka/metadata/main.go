package metadata

// Key typed context key
type Key string

func (k Key) String() string {
	return string(k)
}

const (
	// CtxKafka represents the kafka context type
	CtxKafka = Key("kafka")
)

// Kafka message headers
var (
	HeaderID              = "x-id"
	HeaderAction          = "x-action"
	HeaderStatusCode      = "x-status-code"
	HeaderVersion         = "x-version"
	HeaderParentID        = "x-parent-id"
	HeaderParentTimestamp = "x-parent-timestamp"
)
