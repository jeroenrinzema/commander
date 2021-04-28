package commander

// import (
// 	"context"
// 	"errors"
// 	"testing"
// 	"time"

// 	"github.com/jeroenrinzema/commander/internal/metadata"
// )

// // TestNewResponseWriter tests if able to construct a new resoponse writer
// func TestNewResponseWriter(t *testing.T) {
// 	group, client := NewMockClient()
// 	defer client.Close()

// 	parent := NewMessage("testing", 1, nil, nil)

// 	NewWriter(group, parent)
// 	NewWriter(group, nil)
// }

// // TestWriterProduceCommand tests if able to write a command to the mock group
// func TestWriterProduceCommand(t *testing.T) {
// 	group, client := NewMockClient()
// 	defer client.Close()

// 	action := "testing"

// 	message := NewMessage(action, 1, nil, nil)
// 	writer := NewWriter(group, message)

// 	messages, closing, err := group.NewConsumer(CommandMessage)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	defer closing()

// 	if _, err := writer.Command(action, 1, nil, nil); err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	deadline := time.Now().Add(500 * time.Millisecond)
// 	ctx, cancel := context.WithDeadline(context.Background(), deadline)

// 	defer cancel()

// 	select {
// 	case message := <-messages:
// 		if !message.EOS {
// 			t.Error("unexpected stream")
// 		}

// 		message.Ack()
// 	case <-ctx.Done():
// 		t.Error("the events handle was not called within the deadline")
// 	}
// }

// // TestWriterProduceCommandStream tests if able to write a command to the mock group
// func TestWriterProduceCommandStream(t *testing.T) {
// 	group, client := NewMockClient()
// 	defer client.Close()

// 	action := "testing"

// 	message := NewMessage(action, 1, nil, nil)
// 	writer := NewWriter(group, message)

// 	messages, closing, err := group.NewConsumer(CommandMessage)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	defer closing()

// 	if _, err := writer.CommandStream(action, 1, nil, nil); err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	deadline := time.Now().Add(500 * time.Millisecond)
// 	ctx, cancel := context.WithDeadline(context.Background(), deadline)

// 	defer cancel()

// 	select {
// 	case message := <-messages:
// 		if message.EOS {
// 			t.Error("unexpected EOS")
// 		}

// 		message.Ack()
// 	case <-ctx.Done():
// 		t.Error("the events handle was not called within the deadline")
// 	}
// }

// // TestWriterProduceEvent tests if able to write a successfull event stream
// func TestWriterProduceEvent(t *testing.T) {
// 	group, _ := NewMockClient()
// 	action := "testing"

// 	message := NewMessage(action, 1, nil, nil)
// 	writer := NewWriter(group, message)

// 	messages, closing, err := group.NewConsumer(EventMessage)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	defer closing()
// 	stream := make([]*Message, 10)

// 	go func() {
// 		for range stream {
// 			if _, err := writer.Event(action, 1, nil, nil); err != nil {
// 				t.Error(err)
// 				return
// 			}
// 		}
// 	}()

// 	deadline := time.Now().Add(500 * time.Millisecond)
// 	ctx, cancel := context.WithDeadline(context.Background(), deadline)

// 	defer cancel()

// 	for range stream {
// 		select {
// 		case message := <-messages:
// 			if !message.EOS {
// 				t.Error("unexpected stream")
// 			}

// 			message.Ack()
// 		case <-ctx.Done():
// 			t.Error("the events handle was not called within the deadline")
// 		}
// 	}
// }

// // TestWriterProduceEventStream tests if able to write a event to the mock group
// func TestWriterProduceEventStream(t *testing.T) {
// 	group, client := NewMockClient()
// 	defer client.Close()

// 	action := "testing"

// 	parent := NewMessage(action, 1, nil, nil)
// 	writer := NewWriter(group, parent)

// 	messages, closing, err := group.NewConsumer(EventMessage)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	defer closing()

// 	if _, err := writer.EventStream(action, 1, nil, nil); err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	deadline := time.Now().Add(500 * time.Millisecond)
// 	ctx, cancel := context.WithDeadline(context.Background(), deadline)

// 	defer cancel()

// 	select {
// 	case message := <-messages:
// 		if message.EOS {
// 			t.Error("unexpected EOS")
// 		}

// 		id, _ := metadata.ParentIDFromContext(message.Ctx())
// 		if id != metadata.ParentID(parent.ID) {
// 			t.Error("The event parent does not match the parent id:", id, parent.ID)
// 		}

// 		message.Ack()
// 	case <-ctx.Done():
// 		t.Error("the events handle was not called within the deadline")
// 	}
// }

// // TestWriterProduceErrorEvent tests if able to write a error event to the mock group
// func TestWriterProduceErrorEvent(t *testing.T) {
// 	group, client := NewMockClient()
// 	defer client.Close()

// 	action := "testing"

// 	parent := NewMessage(action, 1, nil, nil)
// 	writer := NewWriter(group, parent)

// 	messages, closing, err := group.NewConsumer(EventMessage)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	defer closing()

// 	if _, err := writer.Error(action, StatusImATeapot, errors.New("test error")); err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	deadline := time.Now().Add(500 * time.Millisecond)
// 	ctx, cancel := context.WithDeadline(context.Background(), deadline)

// 	defer cancel()

// 	select {
// 	case message := <-messages:
// 		if !message.EOS {
// 			t.Error("unexpected stream")
// 		}

// 		id, _ := metadata.ParentIDFromContext(message.Ctx())
// 		if id != metadata.ParentID(parent.ID) {
// 			t.Error("The event parent does not match the parent id:", id, parent.ID)
// 		}

// 		message.Ack()
// 	case <-ctx.Done():
// 		t.Error("the events handle was not called within the deadline")
// 	}
// }

// // TestWriterProduceErrorEventStream tests if able to write a error event to the mock group
// func TestWriterProduceErrorEventStream(t *testing.T) {
// 	group, client := NewMockClient()
// 	defer client.Close()

// 	action := "testing"

// 	parent := NewMessage(action, 1, nil, nil)
// 	writer := NewWriter(group, parent)

// 	messages, closing, err := group.NewConsumer(EventMessage)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	defer closing()

// 	if _, err := writer.ErrorStream(action, StatusImATeapot, errors.New("test error")); err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	deadline := time.Now().Add(500 * time.Millisecond)
// 	ctx, cancel := context.WithDeadline(context.Background(), deadline)

// 	defer cancel()

// 	select {
// 	case message := <-messages:
// 		if message.EOS {
// 			t.Error("unexpected EOS")
// 		}

// 		id, _ := metadata.ParentIDFromContext(message.Ctx())
// 		if id != metadata.ParentID(parent.ID) {
// 			t.Error("The event parent does not match the parent id:", id, parent.ID)
// 		}

// 		message.Ack()
// 	case <-ctx.Done():
// 		t.Error("the events handle was not called within the deadline")
// 	}
// }
