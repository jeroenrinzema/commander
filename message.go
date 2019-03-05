package commander

import "encoding/json"

// Message contains all the nessasery information
type Message struct {
	Topic   Topic                      `json:"topic"`
	Headers map[string]json.RawMessage `json:"headers"`
	Value   []byte                     `json:"value"`
	Key     []byte                     `json:"key"`
	Retries int                        `json:"retries"`
}
