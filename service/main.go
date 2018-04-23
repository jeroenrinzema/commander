package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/mitchellh/mapstructure"
	"github.com/sysco-middleware/commander/commander"

	uuid "github.com/satori/go.uuid"
)

func main() {
	server := &commander.Commander{
		Producer: commander.NewProducer("localhost"),
		Consumer: commander.NewConsumer("localhost", "service"),
	}

	server.Handle("ping", func(command commander.Command) {
		id, _ := uuid.NewV4()

		event := commander.Event{
			Parent: command.ID,
			ID:     id,
			Action: "pong",
		}

		go server.PushEvent(event)
	})

	server.Handle("new_user", func(command commander.Command) {
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

		userID, _ := uuid.NewV4()
		dataset := commander.NewDataset(userID, []commander.Column{
			commander.Column{
				Topic: "user-email",
				Value: data.Email,
			},
			commander.Column{
				Topic: "user-username",
				Value: data.Username,
			},
		})

		go server.PushDataset(dataset)
		go server.PushEvent(event)
	})

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		server.Close()
		os.Exit(0)
	}()

	server.ReadMessages()
}
