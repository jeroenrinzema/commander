package commander

// TopicMode represents the mode of the given topic.
// The mode describes if the given topic is marked for consumption/production/streaming...
type TopicMode int8

// Available topic modes
const (
	ConsumeMode TopicMode = 1 << iota
	ProduceMode
)

// NewTopic constructs a new commander topic for the given name, type, mode and dialect.
func NewTopic(name string, dialect Dialect, t MessageType, m TopicMode) Topic {
	topic := Topic{
		Name:    name,
		Dialect: dialect,
		Type:    t,
		Mode:    m,
	}

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
