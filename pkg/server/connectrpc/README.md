# ConnectRPC server

`Server` serves generated Connect handlers over HTTP/1.1 and h2c. Connect,
gRPC, and gRPC-Web wire protocols remain enabled by the generated handlers.

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
if err := server.Handle(usersv1connect.NewUserServiceHandler(
    service,
    server.HandlerOptions()...,
)); err != nil {
    return err
}
return server.Run(ctx)
```

Registry endpoints must be explicitly advertised; listener addresses such as
`0.0.0.0` are never guessed. `Run` unregisters before draining HTTP requests.
