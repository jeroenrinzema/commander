package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/dialects/kafka"
)

// Available flags
var (
	Brokers = ""
	Version = ""
	Group   = ""
)

func init() {
	commander.Logger.SetOutput(os.Stdout)
	flag.StringVar(&Brokers, "brokers", "127.0.0.1:9092", "Kafka brokers separated by a ,")
	flag.StringVar(&Version, "version", "2.1.1", "Kafka cluster version")
	flag.StringVar(&Group, "group", "", "Optional kafka consumer group")
	flag.Parse()
}

func main() {
	connectionstring := fmt.Sprintf("brokers=%s group=%s initial-offset=newest version=%s", Brokers, Group, Version)
	log.Println("Connecting to Kafka:", connectionstring)

	dialect, err := kafka.NewDialect(connectionstring)
	if err != nil {
		panic(err)
	}

	/**
	 * Commander group definition, multiple groups could be defined and assigned to a single instance.
	 */
	warehouse := commander.NewGroup(
		commander.NewTopic("commands", dialect, commander.CommandMessage, commander.DefaultMode),
		commander.NewTopic("events", dialect, commander.EventMessage, commander.DefaultMode),
	)

	client, err := commander.NewClient(warehouse)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	/**
	 * HandleFunc handles an "Available" command. Once a command with the action "Available" is
	 * processed will a event with the action "created" be produced to the events topic.
	 */
	warehouse.HandleFunc(commander.CommandMessage, "Available", func(writer commander.ResponseWriter, message interface{}) {
		key, err := uuid.NewV4()
		if err != nil {
			// Mark the message to be retried, this will reset the offset of the message topic, parition to the original message offset
			writer.Retry(err)
			return
		}

		// Event: name, version, key, data
		writer.ProduceEvent("Available", 1, key, nil)
	})

	/**
	 * Handle creates a new "Available" command that is produced to the warehouse group command topic.
	 * Once the command is written is a responding event awaited. The responding event has a header
	 * with the parent id set to the id of the received command.
	 */
	http.HandleFunc("/available", func(w http.ResponseWriter, r *http.Request) {
		key, err := uuid.NewV4()
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		command := commander.NewCommand("Available", 1, key, nil)
		event, err := warehouse.SyncCommand(command)

		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(event)
	})

	/**
	 * Async command example
	 */
	http.HandleFunc("/async/available", func(w http.ResponseWriter, r *http.Request) {
		key, err := uuid.NewV4()
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		command := commander.NewCommand("Available", 1, key, nil)
		err = warehouse.AsyncCommand(command)

		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(command)
	})

	log.Println("Http server running at :8080")
	log.Println("Available endpoints:")
	log.Println("GET /available to simulate a 'sync' available command")
	log.Println("GET /async/available to simulate a 'async' available command")

	/**
	 * Setup a http server and close it once a SIGTERM signal is received
	 */
	server := &http.Server{
		Addr: ":8080",
	}

	err = server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
