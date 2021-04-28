package kafka

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

// Initial offset key values
const (
	OffsetNewest = "newest"
	OffsetOldest = "oldest"
)

// Default config value's
var (
	DefaultConnectionTimeout = 5 * time.Second
)

// Config contains all the plausible configuration options
type Config struct {
	Brokers           []string
	Group             string
	Version           sarama.KafkaVersion
	InitialOffset     int64
	ConnectionTimeout time.Duration
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
		return config, errors.New("commander requires at least kafka >= v1.0")
	}

	// The initial offset is given as a string and could be a interger, "newest" and "oldest"
	var initialOffset int64
	initialOffsetValue := values[InitialOffsetKey]

	// Set the a default initial offset value if none is given
	if initialOffsetValue == "" {
		initialOffsetValue = OffsetNewest
	}

	switch initialOffsetValue {
	case OffsetNewest:
		initialOffset = sarama.OffsetNewest
	case OffsetOldest:
		initialOffset = sarama.OffsetOldest
	default:
		offset, err := strconv.ParseInt(initialOffsetValue, 10, 64)
		if err != nil {
			return config, err
		}

		initialOffset = offset
	}

	connectionTimeout, err := time.ParseDuration(values[ConnectionTimeoutKey])
	if err != nil {
		connectionTimeout = DefaultConnectionTimeout
	}

	config.Brokers = strings.Split(values[BrokersKey], ",")
	config.Group = values[GroupKey]
	config.Version = version
	config.InitialOffset = initialOffset
	config.ConnectionTimeout = connectionTimeout

	if len(config.Brokers) < 1 {
		return config, errors.New("at least one broker needs to be specified")
	}

	return config, nil
}
