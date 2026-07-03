# Service registry

`registry` exposes leased registration and a stateful resolver. The etcd implementation hides initial listing, watch recovery, endpoint caching, passive health state, and P2C selection.

Create or reuse the etcd connection through the independent `etcdclient` package, then inject it into the registry backend:

```go
client, err := etcdclient.GetOrCreateClient("main", clientv3.Config{
    Endpoints: []string{"http://127.0.0.1:2379"},
})
if err != nil {
    return err
}
serviceRegistry, err := registryetcd.New(client, registryetcd.Options{})
```

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
outcome := registry.OutcomeFailure
defer func() {
    _ = resolved.Report(registry.Result{
        Outcome: outcome,
        Latency: time.Since(started),
    })
}()
response, err := transport.RoundTrip(requestFor(resolved.Endpoint()))
if err == nil {
    outcome = registry.OutcomeSuccess
}
```

Only connection failures, timeouts, and HTTP/2 transport failures should be reported as failures. A normal HTTP or Connect response is a successful transport result even when it contains a business error.

## Production behavior

- Resolver discovery uses an initial linearizable Get followed by revision-ordered incremental watch updates. Full reloads occur only during initial load, watch recovery, compaction, or rate-limited all-ejected refreshes.
- P2C selection uses per-service random state and atomic health counters. The request path does not allocate candidate slices or create per-request timers.
- Use `Resolution.Endpoint()` on the request path; `Instance()` intentionally deep-copies metadata and is intended for callers that need the full descriptor.
- Callers own the request-result lifecycle and must report every successful resolution exactly once. A `defer` immediately after `Resolve` is recommended so all return paths report a result.
- A single service sweeper removes idle service watches. `ServiceIdleTTL`, `ServiceSweepInterval`, and `RefreshInterval` can be tuned through `registryetcd.Options`.
- `Registry.Stats()` exposes service and registration gauges plus full reload, watch event, decode error, forced refresh, refresh error, watch restart, and service eviction counters.
- Corrupt instance values are isolated and counted instead of stopping discovery for the entire service. Monitor `DecodeErrors` and repair invalid etcd values promptly.

Default resource controls are a 1-second service sweep interval, 10-minute idle service TTL, 1-second forced-refresh cooldown, and 5-second internal etcd operation timeout.
