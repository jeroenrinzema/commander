package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeroenrinzema/commander"
	uuid "github.com/satori/go.uuid"
)

func main() {
	server := newCommander()
	server.Handle("create", createAccount)
	server.ReadMessages()
}

func deleteAccount(command *commander.Command) {
	type user struct {
		Username string `json:"username"`
		Email    string `json:"email"`
	}

	data := &user{}
	err := json.Unmarshal(command.Data, data)

	if err != nil {
		fmt.Println(err)
		return
	}

	res, _ := json.Marshal(data)
	id, _ := uuid.NewV4()

	event := command.NewEvent("deleted", id, res)
	event.Produce()
}

func createAccount(command *commander.Command) {
	type user struct {
		Username string `json:"username"`
		Email    string `json:"email"`
	}

	data := &user{}
	err := json.Unmarshal(command.Data, data)

	if err != nil {
		fmt.Println(err)
		return
	}

	res, _ := json.Marshal(data)
	id, _ := uuid.NewV4()

	event := command.NewEvent("created", id, res)
	event.Produce()
}

func newCommander() *commander.Commander {
	host := os.Getenv("KAFKA_HOST")
	group := os.Getenv("KAFKA_GROUP")

	instance := &commander.Commander{
		Producer: commander.NewProducer(host),
		Consumer: commander.NewConsumer(host, group),
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		instance.Close()
		os.Exit(0)
	}()

	return instance
}
