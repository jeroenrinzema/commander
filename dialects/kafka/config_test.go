package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

// TestNewConfig tests if able to create a new config of the given values
func TestNewConfig(t *testing.T) {
	values := ConnectionMap{
		BrokersKey:           "broker:9092",
		GroupKey:             "group",
		VersionKey:           "1.0.0",
		InitialOffsetKey:     "0",
		ConnectionTimeoutKey: "1s",
	}

	conf, err := NewConfig(values)
	if err != nil {
		t.Fatal(err)
	}

	if len(conf.Brokers) == 0 {
		t.Fatal("No brokers set")
	}

	if conf.Brokers[0] != values[BrokersKey] {
		t.Fatal("Brokers not set")
	}

	if conf.Group != values[GroupKey] {
		t.Fatal("Group not set")
	}

	if conf.Version != sarama.V1_0_0_0 {
		t.Fatal("No version was set")
	}

	if conf.InitialOffset != 0 {
		t.Fatal("No initial offset was set")
	}

	if conf.ConnectionTimeout != 1*time.Second {
		t.Fatal("No connection timeout was set")
	}
}
