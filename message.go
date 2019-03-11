package commander

const (
	// ParentHeader kafka message parent header
	ParentHeader = "cmdr_parent"
	// ActionHeader kafka message action header
	ActionHeader = "cmdr_action"
	// IDHeader kafka message id header
	IDHeader = "cmdr_id"
	// StatusHeader kafka message status header
	StatusHeader = "cmdr_status"
	// VersionHeader kafka message version header
	VersionHeader = "cmdr_version"
	// MetaHeader kafka message meta header
	MetaHeader = "cmdr_meta"
)

// Message contains all the nessasery information
type Message struct {
	Topic   Topic             `json:"topic"`
	Headers map[string]string `json:"headers"`
	Value   []byte            `json:"value"`
	Key     []byte            `json:"key"`
	Retries int               `json:"retries"`
}
