package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander/dialects/kafka/consumer"
	"github.com/jeroenrinzema/commander/dialects/kafka/producer"
	"github.com/jeroenrinzema/commander/types"
)

// Dialect represents the kafka dialect
type Dialect struct {
	Connection Config
	Topics     []types.Topic
	Config     *sarama.Config

	conn     sarama.Client
	consumer *consumer.Client
	producer *producer.Client
}

// NewDialect initializes and constructs a new Kafka dialect
func NewDialect(connectionstring string) (*Dialect, error) {
	values := ParseConnectionstring(connectionstring)
	err := ValidateConnectionKeyVal(values)
	if err != nil {
		return nil, err
	}

	connection, err := NewConfig(values)
	if err != nil {
		return nil, err
	}

	dialect := &Dialect{
		Connection: connection,
		Config:     sarama.NewConfig(),
		consumer:   consumer.NewClient(connection.Brokers, connection.Group),
		producer:   producer.NewClient(),
	}

	dialect.Config.Version = connection.Version
	dialect.Config.Producer.Return.Successes = true

	return dialect, nil
}

// Consumer returns the dialect as consumer
func (dialect *Dialect) Consumer() types.Consumer {
	return dialect.consumer
}

// Producer returns the dialect as producer
func (dialect *Dialect) Producer() types.Producer {
	return dialect.producer
}

// Assigned is called when a topic gets created
func (dialect *Dialect) Assigned(topic types.Topic) {
	dialect.Topics = append(dialect.Topics, topic)
}

// Open opens a kafka consumer and producer
func (dialect *Dialect) Open() error {
	conn, err := sarama.NewClient(dialect.Connection.Brokers, dialect.Config)
	if err != nil {
		return err
	}

	err = dialect.consumer.Connect(conn, dialect.Connection.InitialOffset, dialect.Topics...)
	if err != nil {
		return err
	}

	err = dialect.producer.Connect(conn)
	if err != nil {
		return err
	}

	dialect.conn = conn
	return nil
}

// Close closes the Kafka consumers and producers
func (dialect *Dialect) Close() error {
	var err error

	err = dialect.consumer.Close()
	if err != nil {
		return err
	}

	err = dialect.producer.Close()
	if err != nil {
		return err
	}

	return nil
}

// Healthy returns a boolean that reprisents if the dialect is healthy
func (dialect *Dialect) Healthy() bool {
	if dialect.conn == nil {
		return false
	}

	brokers := dialect.conn.Brokers()
	if len(brokers) == 0 {
		return false
	}

	return true
}
