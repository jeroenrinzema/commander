package zipkin

import "testing"

// TestNewConfig tests if able to create a new config of the given values
func TestNewConfig(t *testing.T) {
	val := "value"

	values := ConnectionMap{
		ZipkinHost:  val,
		ServiceName: val,
	}

	conf, err := NewConfig(values)
	if err != nil {
		t.Fatal(err)
	}

	if conf.ZipkinHost != val {
		t.Fatal("ZipkinHost not set")
	}

	if conf.ServiceName != val {
		t.Fatal("ServiceName not set")
	}
}
