package commander

import (
	"encoding/json"
	"strconv"
)

// NewMockCommandMessage initializes a new mock command message
func NewMockCommandMessage(action string, key string, id string, value string, topic Topic) Message {
	headers := map[string]json.RawMessage{
		ActionHeader: []byte(action),
		IDHeader:     []byte(id),
	}

	message := Message{
		Headers: headers,
		Key:     []byte(key),
		Value:   []byte(value),
		Topic:   topic,
	}

	return message
}

// NewMockEventMessage initializes a new mock event message
func NewMockEventMessage(action string, version int8, parent string, key string, id string, value string, topic Topic) Message {
	headers := map[string]json.RawMessage{
		ActionHeader:  []byte(action),
		ParentHeader:  []byte(parent),
		IDHeader:      []byte(id),
		StatusHeader:  []byte("200"),
		VersionHeader: []byte(strconv.Itoa(int(version))),
	}

	message := Message{
		Headers: headers,
		Key:     []byte(key),
		Value:   []byte(value),
		Topic:   topic,
	}

	return message
}
