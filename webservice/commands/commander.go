package commands

import (
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

	return Commander
}
