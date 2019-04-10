package commander

import (
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

// TestClosingConsumptions test if consumptions get closed properly.
// This function tests if the message does not get delivered before the sleep period has passed.
func TestClosingConsumptions(t *testing.T) {
	group := NewTestGroup()
	client := NewTestClient(group)

	action := "testing"
	version := int8(1)
	delivered := make(chan error, 1)

	group.HandleFunc(EventTopic, action, func(writer ResponseWriter, message interface{}) {
		time.Sleep(100 * time.Millisecond)
		delivered <- nil
	})

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent(action, version, parent, key, []byte("{}"))
	group.ProduceEvent(event)

	client.Close()

	if len(delivered) == 0 {
		t.Fatal("the client did not close safely")
	}
}
