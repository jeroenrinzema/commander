package mock

import (
	"context"
	"testing"
	"time"

	"github.com/jeroenrinzema/commander/types"
)

// TestConsumerConsumption tests if able to consume messages
func TestConsumerConsumption(t *testing.T) {
	dialect := NewDialect()
	topic := types.NewTopic("mock", dialect, types.EventMessage, types.DefaultMode)
	message := types.Message{
		Topic: topic,
		Ctx:   context.Background(),
	}

	sink := make(chan bool)
	messages, err := dialect.Consumer().Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for message := range messages {
			message.Next()
			close(sink)
		}
	}()

	dialect.Producer().Publish(&message)

	timeout, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	select {
	case <-timeout.Done():
		t.Fatal("Timeout reached")
	case <-sink:
	}
}
