package commander

import "github.com/confluentinc/confluent-kafka-go/kafka"

// MockKafkaConsumer mocks a confluent-kafka-go cluster consumer.
// Message consumption could be simulated by emitting messages via the Emit method.
type MockKafkaConsumer struct {
	events chan kafka.Event
}

// Emit emits the given kafka event to all subscribed consumers
func (mock *MockKafkaConsumer) Emit(event kafka.Event) { mock.events <- event }

// Events returns a channel where emitted consumer can be received upon
func (mock *MockKafkaConsumer) Events() chan kafka.Event { return mock.events }

// SubscribeTopics ...
func (mock *MockKafkaConsumer) SubscribeTopics([]string, kafka.RebalanceCb) error { return nil }

// Assign ...
func (mock *MockKafkaConsumer) Assign([]kafka.TopicPartition) error { return nil }

// Unassign ...
func (mock *MockKafkaConsumer) Unassign() error { return nil }

// Close ...
func (mock *MockKafkaConsumer) Close() error { return nil }

// MockKafkaProducer mocks a confluent-kafka-go cluster producer.
// Message consumption could be simulated by emitting messages via the Emit method.
type MockKafkaProducer struct {
	events chan kafka.Event
}

// Produce produce a empty "success" message to the events channel to simulate a successfull
// produced kafka message.
func (mock *MockKafkaProducer) Produce(message *kafka.Message, event chan kafka.Event) error {
	go func() {
		event <- &kafka.Message{}
	}()

	return nil
}

// Close ...
func (mock *MockKafkaProducer) Close() {}
