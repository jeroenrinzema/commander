package options

import "github.com/jeroenrinzema/commander/internal/types"

// NewTopic constructs a new commander topic for the given name, type, mode and dialect.
// If no topic mode is defined is the default mode (consume|produce) assigned to the topic.
func NewTopic(name string, dialect types.Dialect, t types.MessageType, m types.TopicMode) GroupOption {
	if m == 0 {
		m = types.DefaultMode
	}

	return &topic{
		Topic: types.NewTopic(name, dialect, t, m),
	}
}

type topic struct {
	types.Topic
}

func (option *topic) Apply(options *GroupOptions) {
	options.Topics = append(options.Topics, option.Topic)
}
