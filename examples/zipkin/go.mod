module github.com/jeroenrinzema/commander/examples/mock

require (
	github.com/gofrs/uuid v3.2.0+incompatible
	github.com/jeroenrinzema/commander v1.0.0-rc.19
	github.com/jeroenrinzema/commander/middleware/zipkin v0.0.0-20190327214155-799d55233c0c
)

replace github.com/jeroenrinzema/commander => ../../

replace github.com/jeroenrinzema/commander/middleware/zipkin => ../../middleware/zipkin
