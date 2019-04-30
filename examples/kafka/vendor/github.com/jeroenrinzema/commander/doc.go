/*
Package commander is a toolkit for writing event driven applications, aims to be developer friendly.
Commander supports event driven patterns such as CQRS and has support for different "dialects".
Dialects allow Commander to communicate with different protocols.

	import (
		"github.com/jeroenrinzema/commander"
		"github.com/jeroenrinzema/commander/dialects/mock"
	)

	func main() {
		dialect := mock.NewDialect()
		group := commander.NewGroup(
			NewTopic("commands", dialect, commander.CommandMessage, commander.ConsumeMode),
			NewTopic("event", dialect, commander.EventMessage, commander.ConsumeMode|commander.ProduceMode),
		)

		client := commander.NewClient(group)
		defer client.Close()

		group.HandleFunc("example", commander.CommandTopic, func(writer commander.ResponseWriter, message interface{}) {
			writer.ProduceEvent("created", 1, uuid.Nil, nil)
		})

		command := commander.NewCommand("example", 1, uuid.Nil, nil)
		group.ProduceCommand(command)

		// ...
	}
*/
package commander
