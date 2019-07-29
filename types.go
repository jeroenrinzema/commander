package commander

import "github.com/jeroenrinzema/commander/types"

// To avoid circular dependencies are some types/interfaces/structs moved to a seperate package (types).
// In order to still be able to use the types by simply importing commander are the types imported and extended in commander.

// Available message types
const (
	EventMessage   = types.EventMessage
	CommandMessage = types.CommandMessage
)

// Dialect extention of the Dialect type
type Dialect = types.Dialect

// Message a message
type Message = types.Message

// Available topic modes
const (
	ConsumeMode = types.ConsumeMode
	ProduceMode = types.ProduceMode

	DefaultMode = types.DefaultMode
)

// NewTopic constructs a new commander topic for the given name, type, mode and dialect.
// If no topic mode is defined is the default mode (consume|produce) assigned to the topic.
var NewTopic = types.NewTopic

// Topic contains information of a kafka topic
type Topic = types.Topic
