module github.com/jeroenrinzema/commander

require (
	github.com/gofrs/uuid v3.2.0+incompatible
	github.com/jeroenrinzema/commander/dialects/mock v0.0.0-00010101000000-000000000000
	github.com/jeroenrinzema/commander/types v0.0.0-00010101000000-000000000000
)

replace github.com/jeroenrinzema/commander/dialects/mock => ./dialects/mock

replace github.com/jeroenrinzema/commander/dialects/kafka => ./dialects/kafka

replace github.com/jeroenrinzema/commander/types => ./types
