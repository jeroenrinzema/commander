# Commander 🚀
[![GoDoc](https://godoc.org/github.com/jeroenrinzema/commander?status.svg)](https://godoc.org/github.com/jeroenrinzema/commander)
[![Build Status](https://travis-ci.org/jeroenrinzema/commander.svg?branch=master)](https://travis-ci.org/jeroenrinzema/commander)
[![Coverage](https://codecov.io/gh/jeroenrinzema/commander/branch/master/graph/badge.svg)](https://codecov.io/gh/jeroenrinzema/commander)
[![Coverage Report](https://goreportcard.com/badge/github.com/jeroenrinzema/commander)](https://goreportcard.com/report/github.com/jeroenrinzema/commander)

Commander is a high-level toolkit for writing event-driven applications, aims to be developer-friendly. Commander supports event-driven patterns such as CQRS and has support for different "dialects". Dialects allow Commander to communicate with different protocols.

## 📚 Usage and documentation

Please see [godoc](https://godoc.org/github.com/jeroenrinzema/commander) for detailed usage docs. Or check out the [examples](https://github.com/jeroenrinzema/commander/tree/master/examples).

## Getting started

Below is a simple consume, produce example using the [Mock](https://github.com/jeroenrinzema/commander/tree/master/dialects/mock) dialect. For more advanced code check out the examples on [Github](https://github.com/jeroenrinzema/commander/tree/master/examples).

```go
dialect := mock.NewDialect()
group := commander.NewGroup(
	commander.NewTopic("commands", dialect, commander.CommandMessage, commander.ConsumeMode),
	commander.NewTopic("event", dialect, commander.EventMessage, commander.ConsumeMode|commander.ProduceMode),
)

client, err := commander.NewClient(group)
if err != nil {
	// Handle the err
}

group.HandleFunc("example", commander.CommandTopic, func(message *commander.Message, writer commander.Writer) {
	writer.Event("created", 1, nil, nil)
})

command := commander.NewCommand("example", 1, nil, nil)
group.ProduceCommand(command)
```

## Dialects

A dialect is the connector to a given protocol or infrastructure. A dialect needs to be defined when constructing a new commander instance. Commander comes shipped with a `mocking` dialect designed for testing purposes. Check out the dialects [directory](https://github.com/jeroenrinzema/commander/tree/master/dialects) for the available dialects.

## Event driven patterns

**CQRS** Command Query Responsibility Segregation. Is an event-driven pattern segregating read and write objects. Writes (commands) are used to instruct a state change, reads (events) are returned to represent the state change including the state change itself.

**Event Sourcing** ensures that every change to the state of an application is captured in an event object and that these event objects are themselves stored in the sequence they were applied for the same lifetime as the application state itself.

**Bidirectional streaming** where both sides send a sequence of messages using a read-write stream. The two streams operate independently, so clients and servers can read and write in whatever order they like: for example, the server could wait to receive all the client messages before writing its responses, or it could alternately read a message then write a message, or some other combination of reads and writes. The order of messages in each stream is preserved.

## Contributing

Thank you for your interest in contributing to Commander! ❤

Everyone is welcome to contribute, whether it's in the form of code, documentation, bug reports, feature requests, or anything else. We encourage you to experiment with the project and make contributions to help evolve it to meet your needs!

See the [contributing guide](https://github.com/jeroenrinzema/commander/blob/master/CONTRIBUTING.md) for more details.
