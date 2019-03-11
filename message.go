package commander

const (
	// ParentHeader kafka message parent header
	ParentHeader = "parent"
	// ActionHeader kafka message action header
	ActionHeader = "action"
	// IDHeader kafka message id header
	IDHeader = "id"
	// StatusHeader kafka message status header
	StatusHeader = "status"
	// VersionHeader kafka message version header
	VersionHeader = "version"
	// MetaHeader kafka message meta header
	MetaHeader = "meta"
)

// Message contains all the nessasery information
type Message struct {
	Topic   Topic             `json:"topic"`
	Headers map[string]string `json:"headers"`
	Value   []byte            `json:"value"`
	Key     []byte            `json:"key"`
	Retries int               `json:"retries"`
}
