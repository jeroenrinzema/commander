# Dialect

A commander dialects is responsible for the consumption/production of messages.
Check out the [dialect interface](https://github.com/jeroenrinzema/commander/blob/master/dialect.go) to see which methods have to be available to a dialect.

On construction of the commander instance is a connectionstring and available groups passed which is given to the dialect.
The dialect could when nessasery setup/initialize the given groups/connectionstring on for it's targeted protocol (ex: Kafka, RabbitMQ)

Below is a example mocking dialect shown that allowes messages to be consumed and produced in-memory. This is a very simple example and is not safe for concurrent actions.