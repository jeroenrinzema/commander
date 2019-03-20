package kafka

import (
	"errors"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
)

// Initial offset key values
const (
	OffsetNewest = "newest"
	OffsetOldest = "oldest"
)

// Config contains all the plausible configuration options
type Config struct {
	Brokers       []string
	Group         string
	Version       sarama.KafkaVersion
	InitialOffset int64
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
		return config, errors.New("Commander requires at least kafka >= v1.0")
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
		break
	case OffsetOldest:
		initialOffset = sarama.OffsetOldest
		break
	default:
		offset, err := strconv.ParseInt(initialOffsetValue, 10, 64)
		if err != nil {
			return config, err
		}

		initialOffset = offset
		break
	}

	config.Brokers = strings.Split(values[BrokersKey], ",")
	config.Group = values[GroupKey]
	config.Version = version
	config.InitialOffset = initialOffset

	if len(config.Brokers) < 1 {
		return config, errors.New("At least one broker needs to be specified")
	}

	return config, nil
}
