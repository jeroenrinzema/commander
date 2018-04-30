package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/viper"
	"github.com/sysco-middleware/commander"
	"github.com/sysco-middleware/commander/example/service/commands"
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

	server.Handle("create", commands.Create)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		server.Close()
		os.Exit(0)
	}()

	server.ReadMessages()
}
