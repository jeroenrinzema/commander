package mocks

import "github.com/confluentinc/confluent-kafka-go/kafka"

// KafkaConsumer mocks a confluent-kafka-go cluster consumer.
// Message consumption could be simulated by emitting messages via the Emit method.
type KafkaConsumer struct {
	events chan kafka.Event
}

// Emit emits the given kafka event to all subscribed consumers
func (mock *KafkaConsumer) Emit(event kafka.Event) { mock.events <- event }

// Events returns a channel where emitted consumer can be received upon
func (mock *KafkaConsumer) Events() chan kafka.Event { return mock.events }

// SubscribeTopics ...
func (mock *KafkaConsumer) SubscribeTopics([]string, kafka.RebalanceCb) error { return nil }

// Assign ...
func (mock *KafkaConsumer) Assign([]kafka.TopicPartition) error { return nil }

// Unassign ...
func (mock *KafkaConsumer) Unassign() error { return nil }

// Close ...
func (mock *KafkaConsumer) Close() error { return nil }

// KafkaProducer mocks a confluent-kafka-go cluster producer.
// Message consumption could be simulated by emitting messages via the Emit method.
type KafkaProducer struct {
	events chan kafka.Event
}

// Produce produce a empty "success" message to the events channel to simulate a successfull
// produced kafka message.
func (mock *KafkaProducer) Produce(message *kafka.Message, event chan kafka.Event) error {
	go func() {
		event <- &kafka.Message{}
	}()

	return nil
}

// Close ...
func (mock *KafkaProducer) Close() {}
