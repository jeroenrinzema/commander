package commands

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/viper"
	"github.com/sysco-middleware/commander/commander"
)

// Commander holds the currend command instance
var Commander *commander.Commander

// NewCommander create a new commander and store it in the Commander variable
func NewCommander() *commander.Commander {
	host := viper.GetString("kafka.host")
	group := viper.GetString("kafka.group")

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
