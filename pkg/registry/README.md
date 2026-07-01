# Service registry

`registry` exposes leased registration and a stateful resolver. The etcd implementation hides initial listing, watch recovery, endpoint caching, passive health state, and P2C selection.

Instances must use an `http://host:port` endpoint. Configure a Connect-compatible transport for h2c:

```go
protocols := new(http.Protocols)
protocols.SetUnencryptedHTTP2(true)
transport := &http.Transport{Protocols: protocols}
```

Resolve immediately before sending a request and report exactly one result:

```go
resolved, err := resolver.Resolve(ctx, "users.v1")
if err != nil {
    return err
}

started := time.Now()
response, err := transport.RoundTrip(requestFor(resolved.Instance().Endpoint))
outcome := registry.OutcomeSuccess
if err != nil {
    outcome = registry.OutcomeFailure
}
if reportErr := resolved.Report(registry.Result{
    Outcome: outcome,
    Latency: time.Since(started),
}); reportErr != nil {
    return reportErr
}
```

Only connection failures, timeouts, and HTTP/2 transport failures should be reported as failures. A normal HTTP or Connect response is a successful transport result even when it contains a business error.
