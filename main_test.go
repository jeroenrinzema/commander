package commander

import (
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/types"
)

// TestClosingConsumptions test if consumptions get closed properly.
// This function tests if the message does not get delivered before the sleep period has passed.
func TestClosingConsumptions(t *testing.T) {
	group, client := NewMockClient()
	action := "testing"
	version := types.Version(1)

	consuming := make(chan struct{}, 0)
	delivered := make(chan error, 1)

	group.HandleFunc(EventMessage, action, func(writer ResponseWriter, message interface{}) {
		close(consuming)
		time.Sleep(100 * time.Millisecond)
		delivered <- nil
	})

	parent := types.ParentID(uuid.Must(uuid.NewV4()).String())
	event := NewEvent(action, version, parent, nil, nil)
	err := group.ProduceEvent(event)
	if err != nil {
		t.Fatal(err)
	}

	<-consuming

	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}

	if len(delivered) == 0 {
		t.Fatal("the client did not close safely")
	}
}
