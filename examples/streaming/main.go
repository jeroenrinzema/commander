package main

import (
	"time"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/dialects/mock"
)

func main() {
	commander.Logger.SetOutput(os.Stdout)

	dialect := mock.NewDialect()
	group := commander.NewGroup(
		commander.NewTopic("commands", dialect, commander.CommandMessage, commander.ConsumeMode|commander.ProduceMode),
		commander.NewTopic("events", dialect, commander.EventMessage, commander.ConsumeMode|commander.ProduceMode),
	)

	client, _ := commander.NewClient(group)
	defer client.Close()

	/**
	 * HandleFunc handles an "stream" command. Once a command with the action "stream" is
	 * processed will a event stream be started producing one event every 500ms untill 5 events are produced.
	 */
	group.HandleFunc(commander.CommandMessage, "stream", func(writer commander.ResponseWriter, message interface{}) {
		key, err := uuid.NewV4()
		if err != nil {
			return
		}

		for i := 0; i < 4; i++ {
			writer.ProduceEvent("action", 1, key.Bytes(), nil)
			time.Sleep(100 * time.Millisecond)
		}

		writer.ProduceEventEOS("final", 1, key.Bytes(), nil)
	})

	/**
	 * Handle creates a new "stream" command that is produced to the groups writable command topic.
	 * Once the command is written is a responding event awaited. The responding event has a header
	 * with the parent id set to the id of the received command.
	 */
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := uuid.Must(uuid.NewV4()).Bytes()
		command := commander.NewCommand("stream", 1, key, nil)
		
		defer r.Body.Close()

		// Open a single consumer receiving event messages
		timeout := 5 * time.Second
		messages, marked, closing, erro := group.NewConsumerWithDeadline(timeout, commander.EventMessage)

		defer closing()
		go group.AsyncCommand(command)

		// Consume and filter messages based on their event ID.
		// The connection is closed when a timeout is reached of a EOS event is consumed.
	stream:
		for {
			select {
			case err := <-erro:
				w.Write([]byte(err.Error()))
				break stream
			case message := <-messages:
				event := commander.Event{}
				event.Populate(message)
				
				if event.Parent != command.ID {
					marked <- nil
					break
				}

				json.NewEncoder(w).Encode(event)
				marked <- nil
				if event.EOS {
					break stream
				}
			}
		}
	})

	fmt.Println("Http server running at :8080")
	fmt.Println("Send a http request to / to simulate a event stream")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}
