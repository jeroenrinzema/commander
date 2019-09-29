# Kafka example

A simple Commander example application using the Kafka dialect.
Spin up a Kafka cluster and simply run the example.
Multiple options could be defined in the flags.

A sync command is executed when sending a request to `GET /` and an async request is executed when sending a request to `GET /async`.
If no event is returned within a set timeout period (5s) is a timeout error returned.

```bash
$ # Run the application
$ go run main.go -brokers=127.0.0.1:9092
$ # Sync command
$ curl http://127.0.0.1:8080
$ # Async command
$ curl http://127.0.0.1:8080/async
```
