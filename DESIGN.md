# v0.5.0-rc design docs

```golang
// "With..." methods allow to inject options to various
// parts such as "middleware" to dialects, groups and handles.

dialect := kafka.NewDialect(
	commander.WithJSONCodec(),
	commander.WithTimeout(1*time.Second),
)

group := commander.NewGroup(
	commander.WithTimeout(1*time.Second),
	commander.WithJSONCodec(),
	commander.NewTopic("commands", dialect, commander.CommandMessage, commander.ConsumeMode|commander.ProduceMode),
	commander.NewTopic("events", dialect, commander.EventMessage, commander.ConsumeMode|commander.ProduceMode),
)

// Various options could be defined at different levels of the package
group.HandleContext(
	commander.WithAction("create"),
	commander.WithMessageSchema(schema), // Custom message schema for the given handle context (ex: JSON struct)
	commander.WithCallback(callback),
)
```