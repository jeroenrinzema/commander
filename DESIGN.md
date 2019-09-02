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
// which could easily be "unpacked" when needed. The ContextDefinition function
// wraps the given options in a slice.
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

// A context middleware could subscribe to one of the following events:
// before, during and after. These events are all called on their given context (dialect, group, handle)
// Options could be set to define additional information (implementation inspiration: https://github.com/grpc/grpc-go/blob/master/dialoptions.go#L41)
// Different context options could be created such as ServerOption, DialectOption, GroupOption

group.HandleContext(...definition, commander.WithCallback(callback))
```