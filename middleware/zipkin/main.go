package zipkin

import (
	"github.com/jeroenrinzema/commander/internal/types"
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
func New(connectionstring string) (*Controller, error) {
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

	middleware := &Controller{
		Reporter: reporter,
		Tracer:   tracer,
		Config:   config,
	}

	return middleware, nil
}

// Controller represents a Zipkin middleware instance
type Controller struct {
	Reporter reporter.Reporter
	Tracer   *zipkin.Tracer
	Config   Config
}

// BeforeConsume middleware controller
func (controller *Controller) BeforeConsume(next middleware.BeforeConsumeHandlerFunc) middleware.BeforeConsumeHandlerFunc {
	return func(message *types.Message, writer types.Writer) {
		controller.NewConsumeSpan(message)
		defer controller.AfterConsumeSpan(message)
		next(message, writer)
	}
}

// BeforeProduce middleware controller
func (controller *Controller) BeforeProduce(next middleware.BeforeProduceHandlerFunc) middleware.BeforeProduceHandlerFunc {
	return func(message *types.Message) {
		controller.NewProduceSpan(message)
		defer controller.AfterPublishSpan(message)
		next(message)
	}
}

// Close closes the Zipkin reporter
func (controller *Controller) Close() error {
	return controller.Reporter.Close()
}

// NewConsumeSpan starts a new span and stores it in the message context
func (controller *Controller) NewConsumeSpan(message *types.Message) {
	options := []zipkin.SpanOption{
		zipkin.Kind(model.Consumer),
	}

	sc, ok := metadata.ExtractContextFromMessageHeaders(message)
	if ok {
		options = append(options, zipkin.Parent(sc))
	}

	name := "commander.consume." + message.Action
	span := controller.Tracer.StartSpan(name, options...)

	message.NewCtx(metadata.NewSpanConsumeContext(message.Ctx(), span))

	span.Tag(ActionTag, message.Action)
	span.Tag(VersionTag, message.Version.String())
}

// AfterConsumeSpan finishes the stored span in the message context
func (controller *Controller) AfterConsumeSpan(message *types.Message) {
	span, has := metadata.SpanConsumeFromContext(message.Ctx())
	if !has {
		return
	}

	span.Finish()
}

// NewProduceSpan prepares the given message span headers
func (controller *Controller) NewProduceSpan(message *types.Message) {
	parent, has := metadata.SpanConsumeFromContext(message.Ctx())
	if !has {
		return
	}

	span := controller.Tracer.StartSpan("commander.produce", zipkin.Kind(model.Consumer), zipkin.Parent(parent.Context()))

	span.Tag(ActionTag, message.Action)
	span.Tag(VersionTag, message.Version.String())

	message.NewCtx(metadata.NewSpanProduceContext(message.Ctx(), span))
	message.NewCtx(metadata.AppendMessageHeaders(message.Ctx(), span.Context()))
}

// AfterPublishSpan closes the producing span
func (controller *Controller) AfterPublishSpan(message *types.Message) {
	span, has := metadata.SpanProduceFromContext(message.Ctx())
	if !has {
		return
	}

	span.Finish()
}
