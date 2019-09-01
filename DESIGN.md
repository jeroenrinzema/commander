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

// A context definition allows to define a group of options
// which could easily be "unpacked" when needed.
definition := commander.ContextDefinition(
	commander.WithJSONCodec(),
	commander.WithTimeout(1*time.Second),
)

group.HandleContext(
	commander.WithAction("create")
	commander.WithJSONCodec(),
	commander.WithTimeout(1*time.Second),
	commander.WithCallback(callback),
)

group.HandleContext(...definition, commander.WithCallback(callback))
```