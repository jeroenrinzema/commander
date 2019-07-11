package mock

import (
	"time"

	"github.com/jeroenrinzema/commander/types"
)

// Producer a message producer
type Producer struct {
	consumer *Consumer
}

// Publish produces a message to the given topic
func (producer *Producer) Publish(message *types.Message) error {
	message.Timestamp = time.Now()
	producer.consumer.queue <- message
	return nil
}

// Close closes the producer
func (producer *Producer) Close() error {
	return nil
}
