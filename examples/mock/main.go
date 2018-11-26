package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jeroenrinzema/commander"
	uuid "github.com/satori/go.uuid"
)

var group = &commander.Group{
	Topics: []commander.Topic{
		commander.Topic{
			Name:    "commands",
			Type:    commander.CommandTopic,
			Consume: true,
			Produce: true,
		},
		commander.Topic{
			Name:    "events",
			Type:    commander.EventTopic,
			Consume: true,
			Produce: true,
		},
	},
	Timeout: 5 * time.Second,
}

func main() {
	connectionstring := ""
	dialect := &commander.MockDialect{}

	_, err := commander.New(dialect, connectionstring, group)
	if err != nil {
		panic(err)
	}

	group.HandleFunc("example", commander.CommandTopic, func(writer commander.ResponseWriter, message interface{}) {
		writer.ProduceEvent("created", 1, uuid.NewV4(), nil)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		command := commander.NewCommand("example", uuid.NewV4(), nil)
		event, err := group.SyncCommand(command)

		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(event)
	})

	fmt.Println("Http server running at :8080")
	fmt.Println("Send a http request to / to simulate a 'sync' command")

	http.ListenAndServe(":8080", nil)
}
