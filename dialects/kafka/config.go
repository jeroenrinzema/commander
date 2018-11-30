package kafka

import (
	"errors"
	"strings"

	"github.com/Shopify/sarama"
)

// Config contains all the plausible configuration options
type Config struct {
	Brokers []string
	Group   string
	Version sarama.KafkaVersion
}

// NewConfig constructs a Config from the given connection map
func NewConfig(values ConnectionMap) (Config, error) {
	config := Config{}
	version, err := sarama.ParseKafkaVersion(values[VersionKey])
	if err != nil {
		return config, err
	}

	atLeastV1 := version.IsAtLeast(sarama.V1_0_0_0)
	if !atLeastV1 {
		return config, errors.New("Commander requires at least kafka v1.0")
	}

	config.Brokers = strings.Split(values[BrokersKey], ",")
	config.Group = values[GroupKey]
	config.Version = version

	if len(config.Brokers) < 1 {
		return config, errors.New("At least one broker needs to be specified")
	}

	return config, nil
}
