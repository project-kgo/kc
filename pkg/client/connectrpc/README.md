# ConnectRPC client

The client resolves one registry service for every HTTP request and reports
transport health after response headers arrive. Connect business errors are
successful transport results.

```go
trace, err := clientconnect.Trace()
if err != nil {
    return err
}
discovered, err := clientconnect.New(
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

Use `connect.WithGRPC()` or `connect.WithGRPCWeb()` together with the returned
options when the generated client should use a protocol other than Connect.
The resolver remains owned by the caller.
