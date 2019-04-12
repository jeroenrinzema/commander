# Mock dialect

A in-memory mocking dialect. This dialect is the main dialect used by commander to preform unit tests.
The dialect could also be used as a bridge between protocols that have no official dialect support.

## Getting started

No initial configurations are required to setup the mock dialect. Simply construct a new dialect and your good to go.

```go
dialect := mock.NewDialect()
// ...
```