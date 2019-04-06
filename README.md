# Commander
[![GoDoc](https://godoc.org/github.com/jeroenrinzema/commander?status.svg)](https://godoc.org/github.com/jeroenrinzema/commander)
[![Build Status](https://travis-ci.org/jeroenrinzema/commander.svg?branch=master)](https://travis-ci.org/jeroenrinzema/commander)
[![Coverage](https://codecov.io/gh/jeroenrinzema/commander/branch/master/graph/badge.svg)](https://codecov.io/gh/jeroenrinzema/commander)
[![Coverage Report](https://goreportcard.com/badge/github.com/jeroenrinzema/commander)](https://goreportcard.com/report/github.com/jeroenrinzema/commander)

Commander is a toolset for writing event driven applications, aims to be developer friendly. Commander supports event driven patterns such as CQRS and has support for different infastructure "dialects".

## Usage and documentation

Please see [godoc](https://godoc.org/github.com/jeroenrinzema/commander) for detailed usage docs. Or check out the [examples](https://github.com/jeroenrinzema/commander/tree/master/examples).

## Official dialects

- **[Kafka](https://github.com/jeroenrinzema/commander/tree/master/dialects/kafka)** - Commander Kafka consumer/producer build upon the Sarama go Kafka client.

## Examples

For more advanced code check out the examples on [Github](https://github.com/jeroenrinzema/commander/tree/master/examples).

## Getting started

- **Dialects**: A dialect is responsible for the production/consumption of events.
- **Groups**: A group contains the configuration of where the commands/events should be produced or consumed from. Also are group wide configurations such as timeout's defined ina group configuration.

Let's first set up a simple commander group.

```go
var group = commander.Group{
	Topics: []commander.Topic{
		{
			Name: "commands",
			Type: commander.CommandTopic,
			Produce: true,
			Consume: true,
		},
		{
			Name: "events",
			Type: commander.EventTopic,
			Produce: true,
			Consume: true,
		},
	},
	Timeout: 5*time.Second,
}
```

Topics and other various configurations get defined inside a commander group. A group get's attached to a commander instance. Multiple groups/dialects could be defined and work together. Commander tries to not restrict the ways that you could produce/consume your event streams.

Once the event groups are defined should the communication dialect be set up and commander be initialized. Notice that the configured commander groups have to be passed as arguments when initializing the commander instance.

```go
dialect := &commander.MockDialect{}
commander.New(dialect, "", group)

group.HandleFunc("example", commander.CommandTopic, func(writer commander.ResponseWriter, message interface{}) {
	writer.ProduceEvent("created", 1, uuid.Nil, nil)
})

command := commander.NewCommand("example", 1, uuid.Nil, nil)
group.ProduceCommand(command)
```

This example consumes commands with the action `example` and produces at once a event with the action `created` to the event topic. In this example is the [CQRS](https://martinfowler.com/bliki/CQRS.html) pattern used but commander is not limited by it. Commander tries to be flexible and allowes applications to be written in many different ways.

## Dialects

A dialect is the connector to a given protocol or infrastructure. A dialect needs to be defined when constructing a new commander instance. Commander comes shipped with a `mocking` dialect designed for testing purposes. Check out the dialects [directory](https://github.com/jeroenrinzema/commander/tree/master/dialects) for the available dialects.

## Middleware

Middleware allowes actions to be preformed on event(s) or messages to be manipulated. Check out the middleware [directory](https://github.com/jeroenrinzema/commander/tree/master/middleware) for the available middleware controllers.
