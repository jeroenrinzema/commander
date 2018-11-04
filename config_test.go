package commander

import (
	"testing"
)

// TestNewConfig test the creation of a new config
func TestNewConfig(t *testing.T) {
	NewConfig()
}

// TestConfigValidateGroup tests if groups get validated correctly
func TestConfigValidateGroup(t *testing.T) {
	var err error

	config := NewConfig()
	group := &Group{
		Topics: []Topic{
			Topic{
				Name:    "",
				Type:    EventTopic,
				Consume: true,
				Produce: true,
			},
			Topic{
				Name:    "",
				Type:    CommandTopic,
				Consume: true,
				Produce: true,
			},
		},
	}

	err = config.ValidateGroup(group)
	if err != nil {
		t.Error("validate fails while it should not", err)
	}

	group.Topics = append(group.Topics, Topic{
		Name:    "",
		Type:    CommandTopic,
		Consume: true,
		Produce: false,
	})

	err = config.ValidateGroup(group)
	if err == nil {
		t.Error("no error was thrown when having too many command topics marked for consumption")
	}

	group.Topics = group.Topics[:len(group.Topics)-1]

	group.Topics = append(group.Topics, Topic{
		Name:    "",
		Type:    CommandTopic,
		Consume: false,
		Produce: true,
	})

	err = config.ValidateGroup(group)
	if err == nil {
		t.Error("no error was thrown when having too many command topics marked for production")
	}

	group.Topics = group.Topics[:len(group.Topics)-1]

	group.Topics = append(group.Topics, Topic{
		Name:    "",
		Type:    EventTopic,
		Consume: true,
		Produce: false,
	})

	err = config.ValidateGroup(group)
	if err == nil {
		t.Error("no error was thrown when having too many event topics marked for consumption")
	}

	group.Topics = group.Topics[:len(group.Topics)-1]

	group.Topics = append(group.Topics, Topic{
		Name:    "",
		Type:    EventTopic,
		Consume: false,
		Produce: true,
	})

	err = config.ValidateGroup(group)
	if err == nil {
		t.Error("no error was thrown when having too many event topics marked for production")
	}

	group.Topics = group.Topics[:len(group.Topics)-1]
}

// TestConfigValidate tests if groups get validated correctly
func TestConfigValidate(t *testing.T) {
	var err error

	config := NewConfig()

	brokers := []string{"localhost"}
	group := "testing"

	config.Brokers = brokers
	config.Group = group

	err = config.Validate()
	if err != nil {
		t.Error("No error should have been given")
	}

	config.Brokers = []string{}
	err = config.Validate()
	if err == nil {
		t.Error("No error was given while the broker was empty")
	}
	config.Brokers = brokers

	config.Group = ""
	err = config.Validate()
	if err == nil {
		t.Error("No error was given while there were no groups defined")
	}
	config.Group = group
}

// TestConfigAddGroup tests if groups get added correctly
func TestConfigAddGroup(t *testing.T) {
	config := NewConfig()
	group := &Group{}

	config.AddGroups(group)

	if len(config.Groups) != 1 {
		t.Error("A single group did not get added to the config")
	}

	config.AddGroups(group, group)

	if len(config.Groups) != 3 {
		t.Error("Multiple groups did not get added to the config")
	}
}
