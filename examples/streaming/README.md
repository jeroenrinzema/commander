# Streaming example

This example simulates a mock commander client which listens on port `:8080`.
When sending a http request to `/` a event stream is started that will emit 5 events one every 500ms.
If no event is returned within set timeout period (5s) is a timeout error returned.

```bash
$ curl 127.0.0.1:8080
```