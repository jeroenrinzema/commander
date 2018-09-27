package commander

import (
	"testing"
	"time"
)

func NewMockGroup() *Group {
	group := &Group{
		Commander: &Commander{},
		Timeout:   5 * time.Second,
		EventTopic: Topic{
			Name: "testing-events",
		},
		CommandTopic: Topic{
			Name: "testing-commands",
		},
	}

	return group
}

func TestAsyncCommand(t *testing.T) {
	group := NewMockGroup()
	command := NewMockCommand()
	err := group.AsyncCommand(command)

	if err != nil {
		t.Error(err)
	}
}
