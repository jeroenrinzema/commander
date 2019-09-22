package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/dialects/mock"
)

func main() {
	os.Setenv("DEBUG", "true")

	dialect := mock.NewDialect()
	group := commander.NewGroup(
		commander.WithJSONCodec(),
		commander.WithAwaitTimeout(1*time.Second),
		commander.NewTopic("commands", dialect, commander.CommandMessage, commander.ConsumeMode|commander.ProduceMode),
		commander.NewTopic("events", dialect, commander.EventMessage, commander.ConsumeMode|commander.ProduceMode),
	)

	client, _ := commander.NewClient(group)
	defer client.Close()

	type request struct {
		Message string `json:"message"`
	}

	group.HandleContext(
		commander.WithAction("sample"),
		commander.WithMessageType(commander.CommandMessage),
		commander.WithMessageSchema(func() interface{} { return request{} }),
		commander.WithCallback(func(message *commander.Message, writer commander.Writer) {
			message.Schema() // returns decoded map[string]interface{}
		}),
	)

	/**
	 * HandleFunc handles an "example" command. Once a command with the action "example" is
	 * processed will a event with the action "created" be produced to the events topic.
	 */
	group.HandleFunc(commander.CommandMessage, "example", func(message *commander.Message, writer commander.Writer) {
		key, err := uuid.NewV4()
		if err != nil {
			message.Retry(err)
			return
		}

		writer.Event("created", 1, key.Bytes(), nil)
	})

	/**
	 * Handle creates a new "example" command that is produced to the groups writable command topic.
	 * Once the command is written is a responding event awaited. The responding event has a header
	 * with the parent id set to the id of the received command.
	 */
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := uuid.Must(uuid.NewV4()).Bytes()
		command := commander.NewMessage("example", 1, key, nil)
		event, err := group.SyncCommand(command)

		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		event.Next()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(event)
	})

	fmt.Println("Http server running at :8080")
	fmt.Println("Send a http request to / to simulate a 'sync' command")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}
