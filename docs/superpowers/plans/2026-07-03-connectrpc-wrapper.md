# ConnectRPC Wrapper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production-oriented ConnectRPC server/client wrappers with h1+h2c, interceptors, registry integration, recovery, rate limiting, and OpenTelemetry tracing.

**Architecture:** The server owns HTTP lifecycle and leased registration, exposes global handler options, and directly accepts generated handler path/handler pairs. The client uses a registry-aware RoundTripper that resolves and reports each request while generated Connect clients retain their normal API.

**Tech Stack:** Go 1.26, connect-go, OpenTelemetry, x/time/rate, existing registry interfaces.

---

### Task 1: Server lifecycle

**Files:** `pkg/server/connectrpc/server_test.go`, `pkg/server/connectrpc/server.go`, `pkg/server/connectrpc/options.go`

- [ ] Write tests for option validation, global/local handler options, h1+h2c defaults, registration, and shutdown.
- [ ] Run `go test ./pkg/server/connectrpc` and verify RED.
- [ ] Implement the minimal lifecycle and option API.
- [ ] Run `go test ./pkg/server/connectrpc` and verify GREEN.

### Task 2: Registry-aware client

**Files:** `pkg/client/connectrpc/client_test.go`, `pkg/client/connectrpc/client.go`, `pkg/client/connectrpc/transport.go`

- [ ] Write tests for endpoint rewriting, success/failure reporting, request immutability, and protocol defaults.
- [ ] Run `go test ./pkg/client/connectrpc` and verify RED.
- [ ] Implement the client and RoundTripper.
- [ ] Run `go test ./pkg/client/connectrpc` and verify GREEN.

### Task 3: Middleware

**Files:** `pkg/server/connectrpc/middleware_test.go`, `pkg/server/connectrpc/recovery.go`, `pkg/server/connectrpc/ratelimit.go`, `pkg/server/connectrpc/trace.go`

- [ ] Write tests for panic sanitization/callback, per-procedure rate limiting, and trace interceptor construction.
- [ ] Run the package tests and verify RED.
- [ ] Implement recovery, rate limiting, and OpenTelemetry adapters.
- [ ] Run package tests and verify GREEN.

### Task 4: Documentation and verification

**Files:** `pkg/server/connectrpc/README.md`, `pkg/client/connectrpc/README.md`

- [ ] Add concise server and generated-client usage examples.
- [ ] Run `gofmt` on changed Go files.
- [ ] Run focused tests, `go vet ./pkg/server/connectrpc ./pkg/client/connectrpc`, and `go test ./...`.
- [ ] Confirm the only permitted full-suite failure is the pre-existing etcd option-timeout assertion.
