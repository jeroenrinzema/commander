package commander

// Message contains all the nessasery information
type Message struct {
	Topic   Topic    `json:"topic"`
	Headers []Header `json:"headers"`
	Value   []byte   `json:"value"`
	Key     []byte   `json:"key"`
	Retries int      `json:"retries"`
}

// Header represents a message header
type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
