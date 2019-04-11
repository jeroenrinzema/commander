package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander"
)

func main() {
	commander.Logger.SetOutput(os.Stdout)

	dialect := commander.NewMockDialect()
	cart := commander.NewGroup(
		commander.NewTopic("cart-commands", dialect, commander.CommandMessage, commander.DefaultMode),
		commander.NewTopic("cart-events", dialect, commander.EventMessage, commander.DefaultMode),
	)

	warehouse := commander.NewGroup(
		commander.NewTopic("warehouse-commands", dialect, commander.CommandMessage, commander.DefaultMode),
		commander.NewTopic("warehouse-events", dialect, commander.EventMessage, commander.DefaultMode),
	)

	warehouse.Timeout = 2 * time.Second

	client := commander.NewClient(cart, warehouse)
	defer client.Close()

	/**
	 * HandleFunc handles commands with the action "available". Once a available command is received
	 * will a available event be produced.
	 */
	warehouse.HandleFunc(commander.CommandMessage, "Available", func(writer commander.ResponseWriter, message interface{}) {
		command := message.(commander.Command)
		log.Println("> Available")

		id, err := uuid.FromString(string(command.Data))
		if err != nil {
			writer.ProduceError("ParseDataError", commander.StatusBadRequest, nil)
			return
		}

		// ... validate if the item is available

		key, err := uuid.NewV4()
		if err != nil {
			return
		}

		log.Println("< Available")
		writer.ProduceEvent("Available", 1, key, []byte(id.String()))
		return
	})

	/**
	 * HandleFunc handles command with the action "example". Once a command with the action "example" is
	 * processed will a event with the action "created" be produced to the events topic.
	 */
	cart.HandleFunc(commander.CommandMessage, "Purchase", func(writer commander.ResponseWriter, message interface{}) {
		item, _ := uuid.NewV4()
		key, _ := uuid.NewV4()
		log.Println("> Purchase")

		command := commander.NewCommand("Available", 1, key, []byte(item.String()))
		event, err := warehouse.SyncCommand(command)
		if err != nil {
			writer.ProduceError("WarehouseNotAvailable", commander.StatusInternalServerError, err)
			return
		}

		if event.Status != commander.StatusOK {
			writer.ProduceError("NotAvailable", commander.StatusNotFound, err)
			return
		}

		items := []string{item.String()}
		response, _ := json.Marshal(items)

		log.Println("< Purchased")
		writer.ProduceEvent("Purchased", 1, key, response)
		return
	})

	/**
	 * Handle creates a new "example" command that is produced to the groups writable command topic.
	 * Once the command is written is a responding event awaited. The responding event has a header
	 * with the parent id set to the id of the received command.
	 */
	http.HandleFunc("/purchase", func(w http.ResponseWriter, r *http.Request) {
		key, _ := uuid.NewV4()

		command := commander.NewCommand("Purchase", 1, key, nil)
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

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}
