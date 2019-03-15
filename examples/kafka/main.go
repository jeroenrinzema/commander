package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/dialects/kafka"
)

var warehouse = &commander.Group{
	Topics: []commander.Topic{
		{
			Name:    "commands",
			Type:    commander.CommandTopic,
			Consume: true,
			Produce: true,
		},
		{
			Name:    "events",
			Type:    commander.EventTopic,
			Consume: true,
			Produce: true,
		},
	},
	Timeout: 5 * time.Second,
}

func main() {
	commander.Logger.SetOutput(os.Stdout)
	brokers := flag.String("brokers", "127.0.0.1", "Kafka brokers separated by a ,")
	version := flag.String("version", "2.1.1", "Kafka cluster version")
	flag.Parse()

	connectionstring := fmt.Sprintf("brokers=%s group=example version=%s", *brokers, *version)
	log.Println("Connecting to Kafka:", connectionstring)

	dialect := &kafka.Dialect{}
	commander.Logger.SetOutput(os.Stdout)

	/**
	 * When constrcuting a new commander instance do you have to construct a commander.Dialect as well.
	 * A dialect consists mainly of a producer and a consumer that acts as a connector to the wanted infastructure.
	 */
	client, err := commander.New(dialect, connectionstring, warehouse)
	if err != nil {
		panic(err)
	}

	/**
	 * HandleFunc handles an "Available" command. Once a command with the action "Available" is
	 * processed will a event with the action "created" be produced to the events topic.
	 */
	warehouse.HandleFunc(commander.CommandTopic, "Available", func(writer commander.ResponseWriter, message interface{}) {
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
	log.Println("Send a http request to /available to simulate a 'sync' available command")

	/**
	 * Setup a http server and close it once a SIGTERM signal is received
	 */
	server := &http.Server{
		Addr: ":8080",
	}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm

	client.Close()
	server.Close()
}
