# Streaming example

A simple Commander example application using the Mock dialect and bidirectional streaming.
This example simulates the streaming of messages back to the client.
A response is returned once a message marking the end of the stream is returned.

A sync command is executed when sending a request to `GET /`.
If no event is returned within a set timeout period (5s) is a timeout error returned.

```bash
$ # Run the example
$ go run main.go
$ # Execute 
$ curl 127.0.0.1:8080
```