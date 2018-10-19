/*
Package commander gives you a toolset for writing distributed applications following the CQRS and Event Sourcing pattern using Kafka as the event log.

Getting started

Commander makes use of groups that represent a "data set". Events and commands could be consumed and produced to the set topics. Sometimes a topic is not required to be consumed, in order to avoid unnessasery consumption could it be ignored.

```
users := commander.Group{
	CommandTopic: commander.Topic{
		Name: "user-commands",
	},
	EventTopic: commander.Topic{
		Name: "user-events",
	},
}

warehouse := commander.Group{
	CommandTopic: commander.Topic{
		Name: "warehouse-commands",
	},
	EventTopic: commander.Topic{
		Name: "warehouse-events",
		IgnoreConsumption: true,
	},
}
```

The created groups after need to be included to a commander instance. Once included is it plausible to consume and produce events.

```
config := commander.NewConfig()
config.Brokers = []string{"..."}

cmdr := commander.New(&config)
cmdr.AddGroups(users, warehouse)
go cmdr.Consume()

users.OnCommandHandle("NewUser", func(command *commander.Command) *commander.Event {
	// ...

	return command.NewEvent("UserCreated", 1, uuid.NewV4(), nil)
})

users.OnCommandHandle("SendUserSignupGift", func(command *commander.Command) *commander.Event {
	available := warehouse.NewCommand("SendUserSignupGift")
	_, err := warehouse.AsyncCommand(available)
	if err != nil {
		// return ...
	}

	// ...
})
```
*/
package commander
