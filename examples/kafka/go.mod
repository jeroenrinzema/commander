module github.com/jeroenrinzema/commander/examples/kafka

replace github.com/jeroenrinzema/commander => ../../

replace github.com/jeroenrinzema/commander/dialects/kafka => ../../dialects/kafka

replace github.com/jeroenrinzema/commander/middleware/zipkin => ../../middleware/zipkin

require (
	github.com/gofrs/uuid v3.2.0+incompatible
	github.com/jeroenrinzema/commander v0.0.0-20181126162507-b4d62c7740ef
	github.com/jeroenrinzema/commander/dialects/kafka v0.0.0-20181217103823-01d74b882250
	github.com/jeroenrinzema/commander/middleware/zipkin v0.0.0-00010101000000-000000000000
	github.com/openzipkin/zipkin-go v0.1.6
)
