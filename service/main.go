package main

import (
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/sysco-middleware/commander/commander"

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

		type user struct {
			Username string `json:"username"`
			Email    string `json:"email"`
		}

		data := user{}
		mapstructure.Decode(command.Data, &data)

		event := commander.Event{
			Parent: command.ID,
			ID:     id,
			Action: "user_created",
			Data:   data,
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
