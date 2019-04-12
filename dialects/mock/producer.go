package mock

import "github.com/jeroenrinzema/commander"

// Producer a message producer
type Producer struct {
	consumer *Consumer
}

// Publish produces a message to the given topic
func (producer *Producer) Publish(message *commander.Message) error {
	producer.consumer.consumptions.Add(1)
	go producer.consumer.Emit(*message)
	return nil
}

// Close closes the producer
func (producer *Producer) Close() error {
	return nil
}
