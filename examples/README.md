# Commander Examples

When looking into Commander is it adviced to start with the `mock` examples. A mock consumer/producer does not make use of any external service and only depends on packages included in go.

In most examples are the consumer and producer defined in the same instance. When writing event driven application in the CQRS pattern is it adviced to execute the consumers and producers in seperate processes to increase stability and availability.