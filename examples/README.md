# Commander Examples

Various commander consumer/producer examples. Most examples can simply be run by executing `go run .`. Please read the `README.md` inside a example to find out how it works.
In most examples are the consumer and producer defined in the same instance. When writing event driven application in the CQRS pattern is it adviced to execute the consumers and producers in seperate processes to increase stability and availability.

- **[mock](https://github.com/jeroenrinzema/commander/tree/master/examples/mock)** - Simple in-memory consumer/producer
- **[mock-multiple-groups](https://github.com/jeroenrinzema/commander/tree/master/examples/mock-multiple-groups)** - Bit more advanced in-memory consumer/producer using multiple groups
- **[kafka](https://github.com/jeroenrinzema/commander/tree/master/examples/kafka)** - Kafka consumer/producer
- **[zipkin](https://github.com/jeroenrinzema/commander/tree/master/examples/zipkin)** - In-memory consumer/producer using Zipkin as tracer