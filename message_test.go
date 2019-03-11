package commander

import (
	"strconv"
)

// NewMockCommandMessage initializes a new mock command message
func NewMockCommandMessage(action string, key string, id string, value string, topic Topic) Message {
	headers := map[string]string{
		ActionHeader: action,
		IDHeader:     id,
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
	headers := map[string]string{
		ActionHeader:  action,
		ParentHeader:  parent,
		IDHeader:      id,
		StatusHeader:  "200",
		VersionHeader: strconv.Itoa(int(version)),
	}

	message := Message{
		Headers: headers,
		Key:     []byte(key),
		Value:   []byte(value),
		Topic:   topic,
	}

	return message
}
