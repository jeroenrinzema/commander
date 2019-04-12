package zipkin

// Config contains all the plausible configuration options
type Config struct {
	ZipkinHost  string
	ServiceName string
}

// NewConfig constructs a Config from the given connection map
func NewConfig(values ConnectionMap) (Config, error) {
	config := Config{}

	config.ZipkinHost = values[ZipkinHost]
	config.ServiceName = values[ServiceName]

	return config, nil
}
