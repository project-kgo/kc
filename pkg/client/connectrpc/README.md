# ConnectRPC client

The package supports two creation modes:

- `NewDiscovery` or the compatibility alias `New`: resolve one registry service
  for every HTTP request and report transport health after response headers
  arrive. Connect business errors are successful transport results.
- `NewDirect`: connect to a fixed base URL directly, without a resolver.

```go
trace, err := clientconnect.Trace()
if err != nil {
    return err
}
discovered, err := clientconnect.NewDiscovery(
    "users.v1",
    resolver,
    clientconnect.WithInterceptors(trace),
)
if err != nil {
    return err
}
defer discovered.CloseIdleConnections()

client := usersv1connect.NewUserServiceClient(
    discovered.HTTPClient(),
    discovered.BaseURL(),
    discovered.Options()...,
)
```

```go
direct, err := clientconnect.NewDirect(
    "http://127.0.0.1:8080",
    clientconnect.WithInterceptors(trace),
)
if err != nil {
    return err
}
defer direct.CloseIdleConnections()

client := usersv1connect.NewUserServiceClient(
    direct.HTTPClient(),
    direct.BaseURL(),
    direct.Options()...,
)
```

Use `connect.WithGRPC()` or `connect.WithGRPCWeb()` together with the returned
options when the generated client should use a protocol other than Connect.
The resolver remains owned by the caller.
