package commands

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/jeroenrinzema/commander"
)

// Commander holds the currend command instance
var Commander *commander.Commander

// NewCommander create a new commander and store it in the Commander variable
func NewCommander() *commander.Commander {
	host := os.Getenv("KAFKA_HOST")
	group := os.Getenv("KAFKA_GROUP")

	Commander = &commander.Commander{
		Producer: commander.NewProducer(host),
		Consumer: commander.NewConsumer(host, group),
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		Commander.Close()
		os.Exit(0)
	}()

	return Commander
}
