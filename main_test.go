package commander

import (
	"testing"
	"time"

	"github.com/gofrs/uuid"
)

// TestClosingConsumptions test if consumptions get closed properly.
// This function tests if the message does not get delivered before the sleep period has passed.
func TestClosingConsumptions(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	version := int8(1)
	delivered := make(chan error, 1)

	group.HandleFunc(EventMessage, action, func(writer ResponseWriter, message interface{}) {
		time.Sleep(100 * time.Millisecond)
		delivered <- nil
	})

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent(action, version, parent, key, []byte("{}"))
	err := group.ProduceEvent(event)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}

	if len(delivered) == 0 {
		t.Fatal("the client did not close safely")
	}
}
