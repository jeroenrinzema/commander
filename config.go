package commander

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// NewConfig initializes and returns a config struct.
func NewConfig() *Config {
	config := &Config{
		Timeout: 5 * time.Second,
		Kafka:   &kafka.ConfigMap{},
	}

	return config
}

// Config contains all config options for a commander instance.
type Config struct {
	Timeout time.Duration
	Kafka   *kafka.ConfigMap
	Brokers []string
	Group   string
	Groups  []*Group
}

// Validate validates the given config
func (config *Config) Validate() error {
	if len(config.Brokers) == 0 {
		return errors.New("At least 1 kafka broker has to been set")
	}

	if len(config.Group) == 0 {
		return errors.New("No group config name is set")
	}

	return nil
}

// ValidateGroup validates the given group and returns a error if the group is invalid
func (config *Config) ValidateGroup(group *Group) error {
	type types struct {
		commands int
		events   int
	}

	consume := &types{}
	produce := &types{}

	for _, topic := range group.Topics {
		if topic.Consume == true {
			if topic.Type == EventTopic {
				consume.events++
			}

			if topic.Type == CommandTopic {
				consume.commands++
			}
		}

		if topic.Produce == true {
			if topic.Type == EventTopic {
				produce.events++
			}

			if topic.Type == CommandTopic {
				produce.commands++
			}
		}
	}

	if consume.events > 1 {
		return errors.New("To many event topics marked for consumption exist")
	}

	if consume.commands > 1 {
		return errors.New("To many command topics marked for consumption exist")
	}

	if produce.events > 1 {
		return errors.New("To many event topics marked for production exist")
	}

	if produce.commands > 1 {
		return errors.New("To many command topics marked for production exist")
	}

	return nil
}

// AddGroups registeres the given groups at the producer and consumer
func (config *Config) AddGroups(groups ...*Group) {
	config.Groups = append(config.Groups, groups...)
}
