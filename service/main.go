package main

import (
	"commander/service/commander"
	"fmt"

	uuid "github.com/satori/go.uuid"
)

func main() {
	com := commander.Commander{
		Brokers: "localhost",
		Group:   "commands",
	}

	fmt.Println("Consuming commands")

	com.OpenProducer()
	com.ConsumeCommands()

	com.CommandHandle("new_user", func(command commander.Command) {
		id, _ := uuid.NewV4()

		fmt.Println(command)

		event := commander.Event{
			Parent: command.ID,
			ID:     id,
			Action: "user_created",
		}

		com.NewEvent(event)
	})
}
