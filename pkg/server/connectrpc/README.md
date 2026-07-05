# ConnectRPC server

`Server` serves generated Connect handlers over HTTP/1.1 and h2c. Connect,
gRPC, and gRPC-Web wire protocols remain enabled by the generated handlers.

Configure the process-wide OpenTelemetry propagator before constructing trace
interceptors. ConnectRPC then uses the same trace context and baggage formats as
the rest of the application:

```go
otel.SetTextMapPropagator(
	propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	),
)
```

```go
trace, err := connectrpc.Trace()
if err != nil {
    return err
}
limit, err := connectrpc.RateLimit(connectrpc.RateLimitConfig{Rate: 100, Burst: 200})
if err != nil {
    return err
}
server, err := connectrpc.New(
    connectrpc.WithAddress(":8080"),
    connectrpc.WithInterceptors(trace, limit),
    connectrpc.WithHandlerOptions(connectrpc.Recovery(reportPanic)),
    connectrpc.WithRegistry(registrar, connectrpc.RegistrationConfig{
        Service: "users.v1",
        ID: "users-1",
        Endpoint: "http://10.0.0.12:8080",
        TTL: 15 * time.Second,
    }),
)
if err != nil {
    return err
}
if err := server.HandleFactory(func(options ...connect.HandlerOption) (string, http.Handler) {
	return usersv1connect.NewUserServiceHandler(service, options...)
}); err != nil {
	return err
}
return server.Run(ctx)
```

`Trace` trusts incoming remote span context by default, so internal RPC server
spans remain in the caller's trace and use the incoming SpanID as their parent.
Only use this default on trusted internal traffic; terminate or isolate
untrusted public traffic at a gateway.

`HandleFactory` is the recommended registration API. It always applies global
handler options before any handler-local options:

```go
err := server.HandleFactory(
	func(options ...connect.HandlerOption) (string, http.Handler) {
		return usersv1connect.NewUserServiceHandler(service, options...)
	},
	connect.WithInterceptors(serviceInterceptor),
)
```

`Handle` remains available for compatibility and for registering arbitrary
HTTP handlers, but it cannot apply Connect handler options after a handler has
already been constructed. `HandlerOptions` remains available to support code
that must construct generated handlers outside `HandleFactory`.

Registry endpoints must be explicitly advertised; listener addresses such as
`0.0.0.0` are never guessed. `Run` unregisters before draining HTTP requests.
