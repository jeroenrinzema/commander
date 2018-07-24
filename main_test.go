package commander

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	kafkaServers = os.Getenv("TEST_KAFKA_SERVERS")
	commandGroup = os.Getenv("TEST_KAFKA_COMMAND_GROUP")
	consumeGroup = os.Getenv("TEST_KAFKA_CONSUME_GROUP")
)

func TestConsumerCreation(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("no panic was thrown or unable to recover from expected error")
		}
	}()

	// No panic should be thrown
	NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServers,
		"group.id":          commandGroup,
	})

	// A panic is expected
	NewConsumer(nil)
}

func TestProducerCreation(t *testing.T) {
	// No panic should be thrown
	NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServers,
	})
}

func TestProducerCreationExpectingPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("no panic was thrown or unable to recover from expected error")
		}
	}()

	// A panic is expected
	NewProducer(nil)
}

func TestCommandCreation(t *testing.T) {
	type data struct {
		Name string `json:"name"`
	}

	name := "jeroen"
	user := data{name}
	payload, MarshalErr := json.Marshal(user)

	if MarshalErr != nil {
		t.Error(MarshalErr)
	}

	NewCommand("test_action", nil)
	NewCommand("test_action", payload)
	NewCommand("test_action", []byte(name))
}

func TestCommandConsumption(t *testing.T) {

}

func TestEventConsumption(t *testing.T) {

}
