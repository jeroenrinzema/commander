module github.com/jeroenrinzema/commander/examples/kafka

replace github.com/jeroenrinzema/commander => ../../

replace github.com/jeroenrinzema/commander/dialects/kafka => ../../dialects/kafka

require (
	github.com/jeroenrinzema/commander v0.0.0-20181126162507-b4d62c7740ef
	github.com/jeroenrinzema/commander/dialects/kafka v0.0.0-20181130094347-d312953da61e
	github.com/satori/go.uuid v1.2.0
)
