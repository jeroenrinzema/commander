package commander

import "testing"

func TestNewConfig(t *testing.T) {
	NewConfig()
}

func TestNewGroups(t *testing.T) {
	config := NewConfig()
	config.AddGroups(&Group{})

	if len(config.Groups) == 0 {
		t.Error("Group not added")
	}

	config.AddGroups(&Group{}, &Group{})

	if len(config.Groups) != 3 {
		t.Error("Multiple groups not added")
	}
}
