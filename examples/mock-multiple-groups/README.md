# Multiple groups example

A simple Commander example application using the Mock dialect and multiple groups.
This example simulates the executing of multiple groups and awaiting multiple events.

A sync `purchase` command is executed when sending a request to `GET /`.
If no event is returned within a set timeout period (5s) is a timeout error returned.

```bash
$ # Run the example
$ go run main.go
$ # Execute 
$ curl 127.0.0.1:8080
```