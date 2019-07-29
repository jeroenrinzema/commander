package zipkin

import (
	"context"

	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/middleware"
	"github.com/jeroenrinzema/commander/middleware/zipkin/metadata"
	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	reporter "github.com/openzipkin/zipkin-go/reporter"
	httprecorder "github.com/openzipkin/zipkin-go/reporter/http"
)

// Zipkin span tags
const (
	ActionTag  = "commander.message.action"
	VersionTag = "commander.message.version"
)

// New initializes a new Zipkin commander middleware reporter
func New(connectionstring string) (*Zipkin, error) {
	keyval := ParseConnectionstring(connectionstring)
	err := ValidateConnectionKeyVal(keyval)
	if err != nil {
		return nil, err
	}

	config, err := NewConfig(keyval)
	if err != nil {
		return nil, err
	}

	endpoint := &model.Endpoint{
		ServiceName: config.ServiceName,
	}

	reporter := httprecorder.NewReporter(config.ZipkinHost)
	tracer, err := zipkin.NewTracer(reporter, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		return nil, err
	}

	middleware := &Zipkin{
		Reporter: reporter,
		Tracer:   tracer,
		Config:   config,
	}

	return middleware, nil
}

// Zipkin represents a Zipkin middleware instance
type Zipkin struct {
	Reporter reporter.Reporter
	Tracer   *zipkin.Tracer
	Config   Config
}

// Use let the current instance use the given middleware client
func (service *Zipkin) Use(client *middleware.Client) {
	client.Subscribe(commander.BeforeActionConsumption, service.BeforeConsume)
	client.Subscribe(commander.AfterActionConsumption, service.AfterConsume)
	client.Subscribe(commander.BeforePublish, service.BeforePublish)
	client.Subscribe(commander.AfterPublish, service.AfterPublish)
}

// Close closes the Zipkin reporter
func (service *Zipkin) Close() error {
	return service.Reporter.Close()
}

// BeforeConsume starts a new span and stores it in the message context
func (service *Zipkin) BeforeConsume(ctx context.Context, event interface{}) {
	message, ok := event.(*commander.Message)
	if !ok {
		return
	}

	options := []zipkin.SpanOption{
		zipkin.Kind(model.Consumer),
	}

	sc, ok := metadata.ExtractContextFromMessageHeaders(message)
	if ok {
		options = append(options, zipkin.Parent(sc))
	}

	name := "commander.consume." + message.Action
	span := service.Tracer.StartSpan(name, options...)
	ctx = metadata.NewSpanConsumeContext(ctx, span)

	span.Tag(ActionTag, message.Action)
	span.Tag(VersionTag, message.Version.String())
}

// AfterConsume finishes the stored span in the message context
func (service *Zipkin) AfterConsume(ctx context.Context, event interface{}) {
	span, has := metadata.SpanConsumeFromContext(ctx)
	if !has {
		return
	}

	span.Finish()
}

// BeforePublish prepares the given message span headers
func (service *Zipkin) BeforePublish(ctx context.Context, event interface{}) {
	message, ok := event.(*commander.Message)
	if !ok {
		return
	}

	parent, has := metadata.SpanConsumeFromContext(ctx)
	if !has {
		return
	}

	span := service.Tracer.StartSpan("commander.produce", zipkin.Kind(model.Consumer), zipkin.Parent(parent.Context()))

	span.Tag(ActionTag, message.Action)
	span.Tag(VersionTag, message.Version.String())

	ctx = metadata.NewSpanProduceContext(ctx, span)
	ctx = metadata.AppendMessageHeaders(ctx, span.Context())
}

// AfterPublish closes the producing span
func (service *Zipkin) AfterPublish(ctx context.Context, event interface{}) {
	span, has := metadata.SpanProduceFromContext(ctx)
	if !has {
		return
	}

	span.Finish()
}
