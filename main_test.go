package commander

import (
	"context"
	"testing"
	"time"

	"github.com/jeroenrinzema/commander/dialects/mock"
	uuid "github.com/satori/go.uuid"
)

// NewTestClient initializes a new client used for testing
func NewTestClient(groups ...*Group) (*Client, *mock.Dialect) {
	dialect := &mock.Dialect{}
	client, err := New(dialect, "", groups...)
	if err != nil {
		panic(err)
	}

	return client, dialect
}

// TestClosingConsumptions test if consumptions get closed properly
func TestClosingConsumptions(t *testing.T) {
	group := NewTestGroup()
	client, _ := NewTestClient(group)

	action := "testing"
	version := 1
	delivered := make(chan *Event, 1)

	group.HandleFunc(action, EventTopic, func(writer ResponseWriter, message interface{}) {
		event, ok := message.(*Event)
		if !ok {
			t.Error("the received message is not a event")
		}

		time.Sleep(1 * time.Second)
		delivered <- event
	})

	parent := uuid.NewV4()
	key := uuid.NewV4()

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
