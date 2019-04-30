package middleware

import (
	"context"
	"testing"
	"time"
)

// TestEmittingMessage tests if able to emit a message
func TestEmittingMessage(t *testing.T) {
	sink := make(chan bool, 1)
	client := NewClient()

	timeout, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client.Subscribe(BeforeActionConsumption, func(_ *Event) error {
		sink <- true
		return nil
	})

	client.Emit(BeforeActionConsumption, &Event{})

	select {
	case <-timeout.Done():
		t.Error("Timeout reached")
	case <-sink:
	}
}
