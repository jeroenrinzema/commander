package main

import (
	"github.com/spf13/viper"
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

	server := commands.NewCommander()
	server.Handle("create", commands.Create)
	server.ReadMessages()
}
