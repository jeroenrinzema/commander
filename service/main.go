package main

import (
	"fmt"
	"time"

	"github.com/sysco-middleware/commander/service/commander"

	uuid "github.com/satori/go.uuid"
)

func main() {
	com := commander.Commander{
		Brokers: "localhost",
		Group:   "commands",
	}

	fmt.Println("Consuming commands")

	com.OpenProducer()

	commander.CommandHandle("new_user").Start(func(command commander.Command) {
		id, _ := uuid.NewV4()

		fmt.Printf("new_user %s\n", time.Now())

		event := commander.Event{
			Parent: command.ID,
			ID:     id,
			Action: "user_created",
		}

		go com.NewEvent(event)
	})

	commander.CommandHandle("new_email").Start(func(command commander.Command) {
		id, _ := uuid.NewV4()

		fmt.Printf("new_email %s\n", time.Now())

		event := commander.Event{
			Parent: command.ID,
			ID:     id,
			Action: "email_changed",
		}

		go com.NewEvent(event)
	})

	com.ConsumeCommands()
}
