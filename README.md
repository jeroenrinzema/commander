# Commander

Commander gives you a toolset for writing distributed applications following the CQRS and Event Sourcing pattern using Kafka as the event log.

![The pattern](https://github.com/sysco-middleware/commander/wiki/commander-pattern.jpg)

## Getting started

Commander makes use of groups that represent a "data set". Events and commands could be consumed and produced to the set topics. Sometimes a topic is not required to be consumed, in order to avoid unnessasery consumption could it be ignored.

```go
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

```go
package main

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sysco-middleware/commander"
)

func main() {
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
}
```

To get started quickly download/fork the [boilerplate](https://github.com/sysco-middleware/commander-boilerplate).

## Examples

- [Web shop](https://github.com/jeroenrinzema/commander-sock-store-example)

## Usage and documentation

Please see [godoc](https://godoc.org/github.com/sysco-middleware/commander) for detailed usage docs.

## API Overview

- **High performing** - Commander uses Kafka a distributed, fault-tolerant and wicket fast streaming platform as it's transportation layer
- **Privacy first** - Commander offers APIs to be 100% GDPR compliant. All stored events can be easily be encrypted and decrypted to keep the immutable ledger. And offers audit methods to collect all information of a given "key".
- **Developer friendly** - We aim to create developer friendly APIs that get you started quickly

## GDPR (work in progress)

Commander offers various APIs to handle GDPR complaints. To keep the immutable ledger, immutable do we offer the plausibility to encrypt all events. Once a "right to erasure" request needs to be preformed can all data be erased by simply throwing away the key.

## Tests

To run the commander tests make sure that you have set the following environment variables.

```
export TEST_KAFKA_COMMAND_GROUP=TestCommand
export TEST_KAFKA_CONSUME_GROUP=TestConsume
export TEST_KAFKA_SERVERS=kafka:9092
```
