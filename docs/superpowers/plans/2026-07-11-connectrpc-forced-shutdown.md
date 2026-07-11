# ConnectRPC Forced Shutdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在优雅退出超时后强制关闭 ConnectRPC 服务端剩余的普通 HTTP 连接并取消请求 context。

**Architecture:** 保留现有注册注销和 `http.Server.Shutdown` 流程；仅在 `Shutdown` 返回错误时调用 `http.Server.Close` 作为强制收尾。通过 `net.Pipe` 和自定义内存 listener 测试真实 `net/http` 生命周期，避免依赖受限环境中的 TCP 端口。

**Tech Stack:** Go 1.26、`net/http`、`net.Pipe`、标准库 `testing`

## Global Constraints

- 不改变公开 API。
- `shutdownTimeout` 继续作为整个关闭流程的总预算。
- 保留所有关闭阶段的错误，使 `errors.Is(err, context.DeadlineExceeded)` 仍成立。
- `Close` 只能保证关闭连接并取消请求 context，handler 必须自行响应 context 取消。

---

### Task 1: 超时后强制关闭活动连接

**Files:**
- Modify: `pkg/server/connectrpc/server.go:152-158`
- Test: `pkg/server/connectrpc/server_test.go`

**Interfaces:**
- Consumes: `(*http.Server).Shutdown(context.Context) error`、`(*http.Server).Close() error`
- Produces: `(*Server).Run(context.Context) error` 在优雅退出失败时强制关闭连接，并返回合并后的错误

- [ ] **Step 1: 写入失败的回归测试和内存 listener**

在 `server_test.go` 增加一个只交付单个 `net.Pipe` 服务端连接的 listener，并增加 `TestServerRunForceClosesActiveConnectionAfterShutdownTimeout`。测试 handler 等待 `request.Context().Done()`；请求进入后取消运行 context，断言 `Run` 返回值包含 `context.DeadlineExceeded`，并断言 handler 收到取消信号。

```go
type singleConnListener struct {
	conn   chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conn:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *singleConnListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*singleConnListener) Addr() net.Addr { return fakeAddr("pipe") }
```

测试使用 `net.Pipe()`，通过客户端连接写入合法的 HTTP/1.1 请求：

```go
_, _ = io.WriteString(clientConn, "GET /block HTTP/1.1\r\nHost: test\r\n\r\n")
```

- [ ] **Step 2: 运行测试并确认 RED**

Run: `go test -count=1 ./pkg/server/connectrpc -run TestServerRunForceClosesActiveConnectionAfterShutdownTimeout -v`

Expected: FAIL，handler 未在断言期限内收到请求 context 取消；这证明当前 `Shutdown` 超时后活动连接仍然存在。

- [ ] **Step 3: 添加最小强制收尾实现**

将 `server.go` 的退出部分改为：

```go
shutdownErr := s.httpServer.Shutdown(shutdownCtx)
if shutdownErr != nil {
	shutdownErr = errors.Join(shutdownErr, s.httpServer.Close())
}
cause = errors.Join(cause, shutdownErr)
return cause
```

- [ ] **Step 4: 运行回归测试并确认 GREEN**

Run: `go test -count=1 ./pkg/server/connectrpc -run TestServerRunForceClosesActiveConnectionAfterShutdownTimeout -v`

Expected: PASS。

- [ ] **Step 5: 运行目标包完整验证**

Run: `go test -count=1 ./pkg/server/connectrpc ./pkg/client/connectrpc`

Expected: 两个包均 PASS。

Run: `go vet ./pkg/server/connectrpc ./pkg/client/connectrpc`

Expected: 退出码 0，无诊断输出。

- [ ] **Step 6: 检查变更范围**

Run: `git diff --check && git status --short`

Expected: 无 whitespace 错误；仅设计文档修订、计划、`server.go` 和 `server_test.go` 发生变化。

