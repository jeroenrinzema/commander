package commander

import (
	"testing"
	"time"
)

func NewMockGroup() *Group {
	group := &Group{
		Commander: &Commander{},
		Timeout:   5 * time.Second,
		Topics: []Topic{
			EventTopic{
				Name: "testing-events",
			},
			CommandTopic{
				Name: "testing-commands",
			},
		},
	}

	return group
}

func TestAsyncCommand(t *testing.T) {
	group := NewMockGroup()
	command := NewMockCommand()
	topics, err := group.AsyncCommand(command)

	if err != nil {
		t.Error(err)
	}

	if len(topics) != 1 {
		t.Error("command topic is not returned")
	}
}

func TestAsyncCommandMultipleTopics(t *testing.T) {
	group := NewMockGroup()
	command := NewMockCommand()

	group.Topics = append(group.Topics, CommandTopic{Name: "testing-commands-2"})

	topics, err := group.AsyncCommand(command)
	if err != nil {
		t.Error(err)
	}

	if len(topics) != 2 {
		t.Error("not all command topics are returned")
	}
}
