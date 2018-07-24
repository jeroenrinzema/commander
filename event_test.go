package commander

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

func TestEventPopulate(t *testing.T) {
	type data struct {
		Name string `json:"name"`
	}

	key := uuid.NewV4()
	id := uuid.NewV4()
	parent := uuid.NewV4()
	action := "test_action"
	acknowledged := true

	user := data{"jeroen"}
	payload, MarshalErr := json.Marshal(user)

	if MarshalErr != nil {
		t.Error(MarshalErr)
	}

	event := Event{}
	CompleteMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: parent.Bytes(),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: key.Bytes(),
			},
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte(strconv.FormatBool(acknowledged)),
			},
		},
		Key:   id.Bytes(),
		Value: payload,
	}

	var PopulateErr error

	PopulateErr = event.Populate(&CompleteMessage)
	if PopulateErr != nil {
		t.Error(PopulateErr)
	}

	CorruptedParentMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: []byte("faulty"),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: key.Bytes(),
			},
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte(strconv.FormatBool(acknowledged)),
			},
		},
		Key:   id.Bytes(),
		Value: payload,
	}

	PopulateErr = event.Populate(&CorruptedParentMessage)
	if PopulateErr == nil {
		t.Error("no error is thrown when the message parent is corrupted")
	}

	CorruptedKeyMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: parent.Bytes(),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: []byte("faulty"),
			},
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte(strconv.FormatBool(acknowledged)),
			},
		},
		Key:   id.Bytes(),
		Value: payload,
	}

	PopulateErr = event.Populate(&CorruptedKeyMessage)
	if PopulateErr == nil {
		t.Error("no error is thrown when the message key is corrupted")
	}

	CorruptedAcknowledgedMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: parent.Bytes(),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: key.Bytes(),
			},
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte("no boolean"),
			},
		},
		Key:   id.Bytes(),
		Value: payload,
	}

	PopulateErr = event.Populate(&CorruptedAcknowledgedMessage)
	if PopulateErr == nil {
		t.Error("no error is thrown when the message acknowledgement is corrupted")
	}

	CorruptedMessageKeyMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: parent.Bytes(),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: key.Bytes(),
			},
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte(strconv.FormatBool(acknowledged)),
			},
		},
		// Key:   id.Bytes(),
		Value: payload,
	}

	PopulateErr = event.Populate(&CorruptedMessageKeyMessage)
	if PopulateErr == nil {
		t.Error("no error is thrown when the message key is Corrupted")
	}
}
