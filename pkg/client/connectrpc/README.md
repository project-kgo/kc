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
	clientconnect.WithTransportPreset(clientconnect.TransportInternal),
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
	clientconnect.WithTransportPreset(clientconnect.TransportStreaming),
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

## Transport presets

The built-in transport defaults to `TransportInternal`. Choose a preset based
on the endpoint and RPC lifetime:

- `TransportInternal` uses HTTP/1.1 and h2c without environment proxies. It is
  the default for ordinary service-to-service RPCs.
- `TransportPublicTLS` uses HTTP/1.1 and TLS HTTP/2, honors environment proxy
  settings, and uses conservative public-network connection limits.
- `TransportStreaming` uses HTTP/1.1 and h2c without a response-header timeout
  so long-lived internal streams are controlled by their RPC contexts.

The default `http.Client.Timeout` is zero to avoid terminating streams. Set a
deadline on each RPC context, or use `WithHTTPTimeout` when a total client
timeout is appropriate. `WithRoundTripper` completely replaces these built-in
transport settings. `WithProtocols` can override the HTTP/1.1 and h2c flags on
an otherwise built-in transport.
