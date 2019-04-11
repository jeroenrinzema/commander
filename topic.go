package commander

// TopicMode represents the mode of the given topic.
// The mode describes if the given topic is marked for consumption/production/streaming...
type TopicMode int8

// Available topic modes
const (
	ConsumeMode TopicMode = 1 << iota
	ProduceMode

	DefaultMode = ConsumeMode | ProduceMode
)

// NewTopic constructs a new commander topic for the given name, type, mode and dialect.
// If no topic mode is defined is the default mode (consume|produce) assigned to the topic.
func NewTopic(name string, dialect Dialect, t MessageType, m TopicMode) Topic {
	if m == 0 {
		m = DefaultMode
	}

	topic := Topic{
		Name:    name,
		Dialect: dialect,
		Type:    t,
		Mode:    m,
	}

	dialect.Assigned(topic)
	return topic
}

// Topic contains information of a kafka topic
type Topic struct {
	Name    string
	Dialect Dialect
	Type    MessageType
	Mode    TopicMode
}

// HasMode checks if the topic represents the given topic type
func (topic *Topic) HasMode(m TopicMode) bool {
	return topic.Mode&(m) > 0
}
