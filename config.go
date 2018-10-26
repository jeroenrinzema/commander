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
	if len(group.CommandTopic.Name) == 0 {
		return errors.New("The given group has no command topic name set")
	}

	if len(group.EventTopic.Name) == 0 {
		return errors.New("The given group has no event topic name set")
	}

	return nil
}

// AddGroups registeres the given groups at the producer and consumer
func (config *Config) AddGroups(groups ...*Group) {
	config.Groups = append(config.Groups, groups...)
}
