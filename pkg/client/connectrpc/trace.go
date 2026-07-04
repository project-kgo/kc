package connectrpc

import (
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"go.opentelemetry.io/otel/propagation"
)

// Trace creates a client-capable OpenTelemetry interceptor using W3C trace context.
func Trace(options ...otelconnect.Option) (connect.Interceptor, error) {
	defaults := []otelconnect.Option{otelconnect.WithPropagator(propagation.TraceContext{})}
	return otelconnect.NewInterceptor(append(defaults, options...)...)
}
