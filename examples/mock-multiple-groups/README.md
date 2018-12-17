# Multiple groups example

This example simulates the use of multiple groups with a mock commander client which listens on port `:8080`.
When sending a http request to `/purchase` are you simulating a "sync" purchase command. A "sync" command produces a command and awaits the responding event.
If no event is returned within set timeout period (5s) is a timeout error returned. This example tries to discribe the use of multiple groups.
The best practice would be if the `warehouse` group would run in it's own service but for the sake of simplicity did i include it in the same instance.

```bash
$ curl 127.0.0.1:8080/purchase
```

A event is returned with a random generated item in it's value. For example:

```json
{
  "parent": "a5c5bf51-8c66-4923-ba90-e18f4fcd2fc4",
  "headers": {},
  "id": "352907dc-b8c1-4533-85fe-a0f5919bcd8f",
  "action": "purchased",
  "data": [
    "d45efc0e-2de6-4af1-9cbc-a5b3bf3fc7b3"
  ],
  "key": "aa0791cf-7448-45c0-9577-c5a342ff44fe",
  "acknowledged": true,
  "version": 1
}
```