# Zipkin middleware example

A simple Commander example application using the Mock dialect and the Zipkin middleware.
The mock dialect is an in-memory pub/sub requiring no third-party dependencies.

A sync command is executed when sending a request to `GET /`.
If no event is returned within a set timeout period (5s) is a timeout error returned.

```bash
$ # Run the example
$ go run main.go
$ # Execute 
$ curl 127.0.0.1:8080
```