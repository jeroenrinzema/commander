package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander"
)

// Constructing the commander groups
var cart = &commander.Group{
	Topics: []commander.Topic{
		commander.Topic{
			Name:    "cart-commands",
			Type:    commander.CommandTopic,
			Consume: true,
			Produce: true,
		},
		commander.Topic{
			Name:    "cart-events",
			Type:    commander.EventTopic,
			Consume: true,
			Produce: true,
		},
	},
	Timeout: 5 * time.Second,
}

var warehouse = &commander.Group{
	Topics: []commander.Topic{
		commander.Topic{
			Name:    "warehouse-commands",
			Type:    commander.CommandTopic,
			Consume: true,
			Produce: true,
		},
		commander.Topic{
			Name:    "warehouse-events",
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

	/**
	 * When constrcuting a new commander instance do you have to construct a commander.Dialect as well.
	 * A dialect consists mainly of a producer and a consumer that acts as a connector to the wanted infastructure.
	 */
	_, err := commander.New(dialect, connectionstring, cart, warehouse)
	if err != nil {
		panic(err)
	}

	/**
	 * HandleFunc handles commands with the action "available". Once a available command is received
	 * will a available event be produced.
	 */
	warehouse.HandleFunc(commander.CommandTopic, "Available", func(writer commander.ResponseWriter, message interface{}) {
		command, ok := message.(*commander.Command)
		if !ok {
			writer.ProduceError("ParseError", []byte("unable to parse the command"))
			return
		}

		id, err := uuid.FromString(string(command.Data))
		if err != nil {
			writer.ProduceError("ParseDataError", nil)
			return
		}

		// ... validate if the item is available

		key, _ := uuid.NewV4()
		writer.ProduceEvent("Available", 1, key, []byte(id.String()))
	})

	/**
	 * HandleFunc handles command with the action "example". Once a command with the action "example" is
	 * processed will a event with the action "created" be produced to the events topic.
	 */
	cart.HandleFunc(commander.CommandTopic, "Purchase", func(writer commander.ResponseWriter, message interface{}) {
		item, _ := uuid.NewV4()
		key, _ := uuid.NewV4()

		command := commander.NewCommand("Available", key, []byte(item.String()))
		event, err := warehouse.SyncCommand(command)
		if err != nil {
			writer.ProduceError("WarehouseNotAvailable", []byte(err.Error()))
			return
		}

		if event.Status != commander.StatusOK {
			writer.ProduceError("NotAvailable", []byte(err.Error()))
			return
		}

		items := []string{item.String()}
		response, _ := json.Marshal(items)

		writer.ProduceEvent("Purchased", 1, key, response)
	})

	/**
	 * Handle creates a new "example" command that is produced to the groups writable command topic.
	 * Once the command is written is a responding event awaited. The responsing event has a header
	 * with the parent id set to the id of the received command.
	 */
	http.HandleFunc("/purchase", func(w http.ResponseWriter, r *http.Request) {
		key, _ := uuid.NewV4()

		command := commander.NewCommand("Purchase", key, nil)
		event, err := cart.SyncCommand(command)

		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(event)
	})

	fmt.Println("Http server running at :8080")
	fmt.Println("Send a http request to /purchase to simulate a 'sync' purchase command")

	http.ListenAndServe(":8080", nil)
}
