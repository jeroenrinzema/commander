# Mock example

This example simulates a mock commander client which listens on port `:8080`.
When sending a http request to `/` are you simulating a "sync" command. A "sync" command produces a command and awaits the responding event.
If no event is returned within set timeout period (5s) is a timeout error returned.

```bash
$ curl 127.0.0.1:8080
```