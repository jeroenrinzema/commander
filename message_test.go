package commander

import "strconv"

// NewMockCommandMessage initializes a new mock command message
func NewMockCommandMessage(action string, key string, id string, value string, topic Topic) Message {
	message := Message{
		Headers: []Header{
			Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			Header{
				Key:   IDHeader,
				Value: []byte(id),
			},
		},
		Key:   []byte(key),
		Value: []byte(value),
		Topic: topic,
	}

	return message
}

// NewMockEventMessage initializes a new mock event message
func NewMockEventMessage(action string, version int, parent string, key string, id string, value string, topic Topic) Message {
	message := Message{
		Headers: []Header{
			Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			Header{
				Key:   IDHeader,
				Value: []byte(id),
			},
			Header{
				Key:   ParentHeader,
				Value: []byte(parent),
			},
			Header{
				Key:   VersionHeader,
				Value: []byte(strconv.Itoa(version)),
			},
			Header{
				Key:   AcknowledgedHeader,
				Value: []byte("true"),
			},
		},
		Key:   []byte(key),
		Value: []byte(value),
		Topic: topic,
	}

	return message
}
