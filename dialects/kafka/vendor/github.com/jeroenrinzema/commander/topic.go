package commander

// TopicType represents the various topic types
type TopicType int8

// Represents the various topic types
const (
	EventTopic   TopicType = 1
	CommandTopic TopicType = 2
)

// Topic contains information of a kafka topic
type Topic struct {
	Name    string
	Type    TopicType
	Produce bool
	Consume bool
}
