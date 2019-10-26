package types

// TopicMode represents the mode of the given topic.
// The mode describes if the given topic is marked for consumption/production/streaming...
type TopicMode int8

// Available topic modes
const (
	ConsumeMode TopicMode = 1 << iota
	ProduceMode

	DefaultMode = ConsumeMode | ProduceMode
)

// Topic represents a subject for a dialect including it's
// consumer/producer mode.
type Topic interface {
	// Dialect returns the topic Dialect
	Dialect() Dialect
	// Type returns the topic type
	Type() MessageType
	// Mode returns the topic mode
	Mode() TopicMode
	// HasMode checks if the topic represents the given topic type
	HasMode(TopicMode) bool
	// Name returns the topic name
	Name() string
}

// NewTopic constructs a new commander topic for the given name, type, mode and dialect.
// If no topic mode is defined is the default mode (consume|produce) assigned to the topic.
func NewTopic(name string, dialect Dialect, t MessageType, m TopicMode) Topic {
	topic := &topic{
		name:     name,
		dialect:  dialect,
		messages: t,
		mode:     m,
	}

	return topic
}

// Topic interface implementation
type topic struct {
	name     string
	dialect  Dialect
	messages MessageType
	mode     TopicMode
}

func (topic *topic) Dialect() Dialect {
	return topic.dialect
}

func (topic *topic) Type() MessageType {
	return topic.messages
}

func (topic *topic) Mode() TopicMode {
	return topic.mode
}

func (topic *topic) HasMode(m TopicMode) bool {
	return topic.mode&(m) > 0
}

func (topic *topic) Name() string {
	return topic.name
}
