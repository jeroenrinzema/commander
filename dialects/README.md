# Dialect

A commander dialects is responsible for the consumption/production of messages.
Check out the [dialect interface](https://github.com/jeroenrinzema/commander/blob/master/dialect.go) to see which methods have to be available to a dialect.

On construction of the commander instance is a connectionstring and available groups passed which is given to the dialect.
The dialect could when nessasery setup/initialize the given groups/connectionstring on for it's targeted protocol (ex: Kafka, RabbitMQ)

Below is a example mocking dialect shown that allowes messages to be consumed and produced in-memory. This is a very simple example and is not safe for concurrent actions.

## Dialect

```golang
// MockDialect a in-memory mocking dialect
type MockDialect struct {
	Consumer *MockConsumer
	Producer *MockProducer
}

func (dialect *MockDialect) Open(connectionstring string, groups ...*Group) (Consumer, Producer, error) {
	consumer := &MockConsumer{
		subscriptions: make([]chan *commander.Message),
	}

	producer := &MockProducer{
		consumer,
	}

	dialect.Consumer = consumer
	dialect.Producer = producer

	return consumer, producer, nil
}

func (dialect *MockDialect) Healthy() bool {
	return true
}

func (dialect *MockDialect) Close() error {
	return nil
}
```

## Consumer

```golang
// MockSubscription mock message subscription
type MockSubscription struct {
	messages chan *Message
	marked   chan error
}

// MockConsumer consumes messages and emits them to the subscribed channels
type MockConsumer struct {
	subscriptions []chan *commander.Message
}

// Emit emits a message to all subscribers of the given topic
func (consumer *MockConsumer) Emit(message *Message) {
  for _, subscription := range consumer.subscriptions {
    subscription <- message
  }
}

// Subscribe subscribes to the given topics and returs a message channel
func (consumer *MockConsumer) Subscribe(topics ...Topic) (<-chan *Message, chan<- error, error) {
	subscription := &MockSubscription{
		messages: make(chan *Message, 1),
		marked:   make(chan error, 1),
	}

  consumer.subscriptions = append(consumer.subscriptions, subscription)
	return subscription.messages, subscription.marked, nil
}

// Unsubscribe unsubscribes the given consumer channel (if found) from the subscription list
func (consumer *MockConsumer) Unsubscribe(channel <-chan *Message) error {
	for index, subscriptions := range consumer.subscriptions {
		if subscription.messages == channel {
			consumer.subscriptions = append(consumer.subscriptions[:index], consumer.subscriptions[index+1:]...)
		}
	}

	return nil
}

// Close closes the kafka consumer
func (consumer *MockConsumer) Close() error {
	return nil
}
```

## Producer

```golang
// MockProducer emits messages to the attached consumer
type MockProducer struct {
	consumer *MockConsumer
}

// Publish publishes the given message
func (producer *MockProducer) Publish(message *Message) error {
	go producer.consumer.Emit(message)
	return nil
}

// Close closes the kafka producer
func (producer *MockProducer) Close() error {
	return nil
}
```
