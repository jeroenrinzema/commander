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
func NewMockEventMessage(action string, version int8, parent string, key string, id string, value string, topic Topic) Message {
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
				Value: []byte(strconv.Itoa(int(version))),
			},
			Header{
				Key:   StatusHeader,
				Value: []byte("200"),
			},
		},
		Key:   []byte(key),
		Value: []byte(value),
		Topic: topic,
	}

	return message
}
