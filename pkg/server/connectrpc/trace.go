package connectrpc

import (
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
)

func Trace(options ...otelconnect.Option) (connect.Interceptor, error) {
	defaults := []otelconnect.Option{otelconnect.WithTrustRemote()}
	return otelconnect.NewInterceptor(append(defaults, options...)...)
}
