# Commander

Commander gives you a toolset for writing distributed applications following the CQRS and Event Sourcing architecture using Kafka as a event log. The commander is inspired by the [talk](https://www.youtube.com/watch?v=B1-gS0oEtYc&t) and [architecture](https://github.com/capitalone/cqrs-manager-for-distributed-reactive-services/blob/master/doc/architecture.png) given by Bobby Calderwood.

![The pattern](https://github.com/sysco-middleware/commander/wiki/commander-pattern.jpg)

## Getting started

```go
package main

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sysco-middleware/commander"
)

func main() {
	users := commander.Group{
		Topics: []commander.Topic{
			commander.CommandTopic{
				Name: "user-commands",
			},
			commander.EventTopic{
				Name: "user-events",
			},
		},
		HandleCommands: true,
	}

	warehouse := commander.Group{
		Topics: []commander.Topic{
			commander.CommandTopic{
				Name: "warehouse-commands",
			},
			commander.EventTopic{
				Name: "warehouse-events",
			},
		},
		SyncCommands: true,
	}

	config := commander.NewConfig()
	config.Brokers = []string{"..."}
	config.AddGroups(users, warehouse)

	cmdr := commander.New(&config)
	go cmdr.Consume()

	users.OnCommandHandle("NewUser", func(command *commander.Command) *commander.Event {
		// ...

		return command.NewEvent("UserCreated", 1, uuid.NewV4(), nil)
	})

	users.OnCommandHandle("CheckUserWarehouse", func(command *commander.Command) *commander.Event {
		available := warehouse.NewCommand("CheckUsernameAvailable")
		event, err := warehouse.SyncCommand(available)
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

## The architecture
All services inside commander are triggered by events and commands which are immutable. The commander pattern exists out of 4 layers. Every layer has it's own responsibilities and contain different parts of your application.

- **Web service** - This layer is accessible from the outside. The main responsibility is to preform queries on projections or to write commands to the event log. Optionally could this layer authenticate incoming requests.
- **Event log (Kafka)** - The event log is the communication layer in between the web service layer and the business logic layer. Kafka is used in Commander as the event log. All messages passed through the event log are immutable.
- **Business logic** - The business logics layer consumes commands/events to process them. There are two types of consumers that could exists in the business logic layer. The command processor processes commands received from the "commands" topic and generates a resulting event. This event could be a error or the resulting generated data. The projector processes events received from the "events" topic. A projector creates a projection of the consumed events. This projection could be consumed by the web service layer. Command processes and projector processes should never share their states between one another. If a command process requires to preform a validation/query on the latest state should he do it on it's own state.
- **Datastore and projections** - This layer contains sets of states that could be used to query upon. Every service could have it's own projection created of the consumed commands/events.

## API Overview
- **High performing** - Commander uses Kafka a distributed, fault-tolerant and wicket fast streaming platform as it's transportation layer
- **Encryption (work in progress)** - All stored events can be easily encrypted and decrypted
- **Developer friendly** - We aim to create developer friendly APIs that get you started quickly

## State

Every service can hold it's own state/view of the source (events). A state is required to preform queries on the current state of the produced/consumed events. The state could for example be used to validate uniqueness or fetch the current state of a dataset.

Command processes and projector processes should never share their states between one another. If a command process requires to preform a validation/query on the latest state should he do it on it's own state.

## GDPR (work in progress)

Commander offers various APIs to handle GDPR complaints. To keep the immutable ledger, immutable do we offer the plausibility to encrypt all events. Once a "right to erasure" request needs to be preformed can all data be erased by simply throwing away the key.

## Tests

To run the commander tests make sure that you have set the following environment variables.

```
export TEST_KAFKA_COMMAND_GROUP=TestCommand
export TEST_KAFKA_CONSUME_GROUP=TestConsume
export TEST_KAFKA_SERVERS=kafka:9092
```
