# Commander

Commander is a toolset for writing event driven applications, aims to be developer friendly. Commander supports event driven patterns such as CQRS and has support for different infastructure "dialects".

## Usage and documentation

Please see [godoc](https://godoc.org/github.com/jeroenrinzema/commander) for detailed usage docs. Or check out the [examples](https://github.com/jeroenrinzema/commander/tree/master/examples).

## Getting started

Commander mainly exists out of dialects and groups that connect and communicate with one another.

- **Dialects**: A dialect is responsible for it's consumers and producers. It creates a connector to a piece of infastructure that supports some form of event streaming.
- **Groups**: A group contains all the information for commander to setup it's consumers and producers. Commands and Events are consumed/written from/to groups.

Multiple groups/dialects could be defined and work together. Commander tries to not restrict the ways that you could produce/consume your event streams.
An good example is to create a commander instance with a mock dialect.

```go
dialect := &commander.MockDialect{}
commander.New(dialect, "", group)

group.HandleFunc("example", commander.CommandTopic, func(writer commander.ResponseWriter, message interface{}) {
	writer.ProduceEvent("created", 1, uuid.NewV4(), nil)
})
```

This example consumes commands with the action `example` and produces at once a event with the action `created` to the event topic.
The CQRS pattern is used in this example but commander is not limited only to it. Commander allowes applications to be written in many different ways.
Checkout the [multiple groups](https://github.com/jeroenrinzema/commander/tree/master/examples/multiple-groups) example.
