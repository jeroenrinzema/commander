package main

import "event-driven-architecture/service/commander"

func main() {
	com := commander.Commander{
		Brokers: "localhost",
		Group:   "commands",
	}

	com.ConsumeCommands()
}
