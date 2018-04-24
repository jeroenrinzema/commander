package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
	"github.com/sysco-middleware/commander/commander"
)

func main() {
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")
	viper.SetConfigName("default")

	err := viper.ReadInConfig()

	if err != nil {
		panic(err)
	}

	host := viper.GetString("kafka.host")
	group := viper.GetString("kafka.group")

	server := &commander.Commander{
		Producer: commander.NewProducer(host),
		Consumer: commander.NewConsumer(host, group),
	}

	server.Handle(commander.CommandCreate, func(command *commander.Command) {
		type user struct {
			Username string `json:"username"`
			Email    string `json:"email"`
		}

		data := &user{}
		err := json.Unmarshal(command.Data, data)

		if err != nil {
			fmt.Println(err)
			return
		}

		res, _ := json.Marshal(data)
		id, _ := uuid.NewV4()

		event := command.NewEvent(commander.EventCreated, id, res)
		server.ProduceEvent(event)
	})

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		server.Close()
		os.Exit(0)
	}()

	server.ReadMessages()
}
