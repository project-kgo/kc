# ConnectRPC 超时强制收尾设计

## 目标

服务端收到退出信号后，先在 `shutdownTimeout` 内等待活动 HTTP 请求自然结束；超过该期限时，强制关闭剩余普通 HTTP 连接，确保 `Server.Run` 返回前这些请求不再继续运行。

## 行为

1. 保持现有顺序：先关闭服务注册，再调用 `http.Server.Shutdown` 停止接收新连接并等待活动连接。
2. `Shutdown` 成功时不执行额外关闭操作。
3. `Shutdown` 返回错误（包括 context 超时）时调用 `http.Server.Close`，强制关闭剩余活动连接。
4. 使用 `errors.Join` 保留原始运行错误、注册关闭错误、优雅退出错误和强制关闭错误；调用方仍可通过 `errors.Is` 判断 `context.DeadlineExceeded`。
5. 不改变现有 timeout 配置、注册中心关闭顺序或公开 API。

## 测试

增加一个使用真实监听器和阻塞 handler 的回归测试：请求进入 handler 后取消服务端运行 context，验证优雅退出超时会令 `Run` 返回 `context.DeadlineExceeded`，并且强制关闭会取消请求 context，使 handler 在 `Run` 返回前结束。

