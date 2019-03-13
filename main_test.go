package commander

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid"
)

// NewTestClient initializes a new client used for testing
func NewTestClient(groups ...*Group) *Client {
	dialect := &MockDialect{}
	client, err := New(dialect, "", groups...)
	if err != nil {
		panic(err)
	}

	return client
}

// TestClosingConsumptions test if consumptions get closed properly
func TestClosingConsumptions(t *testing.T) {
	group := NewTestGroup()
	client := NewTestClient(group)

	action := "testing"
	version := int8(1)
	delivered := make(chan *Event, 1)

	group.HandleFunc(EventTopic, action, func(writer ResponseWriter, message interface{}) error {
		event, ok := message.(*Event)
		if !ok {
			t.Error("the received message is not a event")
		}

		time.Sleep(1 * time.Second)
		delivered <- event
		return nil
	})

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent(action, version, parent, key, []byte("{}"))
	group.ProduceEvent(event)

	client.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-ctx.Done():
	case <-delivered:
		t.Error("the client did not close safely")
	}
}
