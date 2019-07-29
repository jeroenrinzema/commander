package middleware

import (
	"context"
	"testing"
	"time"
)

// EventType used for testing purposes
type EventType string

// Available event types used for testing
const (
	Before = EventType("before")
	After  = EventType("after")
)

// TestEmittingMessage tests if able to emit a message
func TestEmittingMessage(t *testing.T) {
	sink := make(chan bool, 1)
	client := NewClient()

	timeout, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client.Subscribe(Before, func(ctx context.Context, message interface{}) {
		sink <- true
	})

	client.Emit(context.Background(), Before, nil)

	select {
	case <-timeout.Done():
		t.Fatal("Timeout reached")
	case <-sink:
	}
}
