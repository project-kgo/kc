package connectrpc

import (
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
)

// Trace creates a client-capable OpenTelemetry interceptor using the global
// OpenTelemetry text map propagator unless explicitly overridden by an option.
func Trace(options ...otelconnect.Option) (connect.Interceptor, error) {
	return otelconnect.NewInterceptor(options...)
}
