package commander

import (
	"context"

	"github.com/jeroenrinzema/commander/metadata"
	"github.com/jeroenrinzema/commander/types"
)

// NewMockCommandMessage initializes a new mock command message
func NewMockCommandMessage(action string, key types.Key, id string, data []byte, topic Topic) *Message {
	message := &Message{
		ID:     id,
		Action: action,
		Key:    key,
		Data:   data,
		Topic:  topic,
		Ctx:    context.Background(),
	}

	return message
}

// NewMockEventMessage initializes a new mock event message
func NewMockEventMessage(action string, version types.Version, parent types.ParentID, key types.Key, id string, data []byte, topic Topic) *Message {
	message := &Message{
		ID:      id,
		Action:  action,
		Version: version,
		Key:     key,
		Data:    data,
		Topic:   topic,
		Ctx:     context.Background(),
	}

	message.Ctx = metadata.NewParentIDContext(message.Ctx, parent)
	message.Ctx = metadata.NewStatusCodeContext(message.Ctx, types.StatusOK)

	return message
}
