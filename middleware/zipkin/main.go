package zipkin

import (
	"github.com/jeroenrinzema/commander/internal/types"
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

// Middleware handles the middleware handler request for every consume
func (controller *Zipkin) Middleware(next types.HandlerFunc) types.HandlerFunc {
	return func(message *types.Message, writer types.Writer) {
		controller.Before(message)
		defer controller.After(message)
		next(message, writer)
	}
}

// Close closes the Zipkin reporter
func (controller *Zipkin) Close() error {
	return controller.Reporter.Close()
}

// Before starts a new span and stores it in the message context
func (controller *Zipkin) Before(message *types.Message) {
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

// After finishes the stored span in the message context
func (controller *Zipkin) After(message *types.Message) {
	span, has := metadata.SpanConsumeFromContext(message.Ctx())
	if !has {
		return
	}

	span.Finish()
}

// // BeforePublish prepares the given message span headers
// func (service *Zipkin) BeforePublish(ctx context.Context, event interface{}) {
// 	message, ok := event.(*commander.Message)
// 	if !ok {
// 		return
// 	}

// 	parent, has := metadata.SpanConsumeFromContext(ctx)
// 	if !has {
// 		return
// 	}

// 	span := service.Tracer.StartSpan("commander.produce", zipkin.Kind(model.Consumer), zipkin.Parent(parent.Context()))

// 	span.Tag(ActionTag, message.Action)
// 	span.Tag(VersionTag, message.Version.String())

// 	ctx = metadata.NewSpanProduceContext(ctx, span)
// 	ctx = metadata.AppendMessageHeaders(ctx, span.Context())
// }

// // AfterPublish closes the producing span
// func (service *Zipkin) AfterPublish(ctx context.Context, event interface{}) {
// 	span, has := metadata.SpanProduceFromContext(ctx)
// 	if !has {
// 		return
// 	}

// 	span.Finish()
// }
