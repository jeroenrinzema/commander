package commands

import "github.com/sysco-middleware/commander/commander"

// Commander holds the currend command instance
var Commander *commander.Commander

// NewCommander create a new commander and store it in the Commander variable
func NewCommander() *commander.Commander {
	Commander = &commander.Commander{
		Producer: commander.NewProducer("localhost"),
		Consumer: commander.NewConsumer("localhost", "commands"),
	}

	return Commander
}
