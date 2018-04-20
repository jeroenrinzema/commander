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

	go com.ConsumeCommands()

	com.CommandHandle("new_user", func(command commander.Command) {
		fmt.Println("Command received: new_user,", command)

		id, _ := uuid.NewV4()

		event := commander.Event{
			Parent: command.ID,
			ID:     id,
			Action: "user_created",
		}

		fmt.Println("Sending response event.")
		com.NewEvent(event)
	})
}
