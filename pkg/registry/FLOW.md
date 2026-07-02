# 服务注册、发现与负载均衡流程

```mermaid
flowchart TB
    subgraph Provider["服务提供方"]
        P1["启动服务实例"]
        P2["Register(service, id, endpoint, ttl)"]
        P3["维持 Lease KeepAlive"]
    end

    subgraph Etcd["etcd"]
        E1["创建 Lease"]
        E2["写入服务实例<br/>/prefix/service/instance-id"]
        E3["实例 JSON<br/>endpoint + metadata"]
        E4["Lease 失效<br/>自动删除实例"]
    end

    subgraph Registry["Registry / Resolver"]
        D1["首次 Resolve(service)"]
        D2["全量 Get 当前实例<br/>记录 etcd revision"]
        D3["启动增量 Watch<br/>从 revision + 1 开始"]
        D4["维护本地实例缓存"]
        D5["维护健康状态<br/>EWMA / inflight / failures"]
        D6["P2C 选择两个候选"]
        D7["比较评分<br/>EWMA × (inflight + 1)"]
        D8["返回评分较低实例"]
    end

    subgraph Caller["Connect RPC 调用方"]
        C1["Resolve(service)"]
        C2["获得 Resolution.Endpoint()"]
        C3["发送 h2c RPC 请求"]
        C4["Report(outcome, latency)"]
    end

    subgraph Health["健康反馈"]
        H1["成功"]
        H2["失败"]
        H3["清零连续失败<br/>更新延迟 EWMA"]
        H4["累加连续失败"]
        H5{"达到失败阈值？"}
        H6["摘除 30 秒"]
        H7["半开探测"]
        H8{"探测成功？"}
    end

    subgraph Janitor["统一清理器"]
        J1["每秒执行"]
        J2["清理超时未 Report 的请求"]
        J3["修正 inflight / 半开状态"]
        J4["回收长期未使用的服务缓存"]
        J5["停止对应 etcd Watch"]
    end

    P1 --> P2
    P2 --> E1
    E1 --> E2
    E2 --> E3
    P2 --> P3
    P3 -. 续租 .-> E1
    P3 -. 停止或故障 .-> E4

    C1 --> D1
    D1 --> D2
    D2 --> D4
    D2 --> D3
    E2 -. PUT事件 .-> D3
    E4 -. DELETE事件 .-> D3
    D3 --> D4

    D4 --> D6
    D5 --> D6
    D6 --> D7
    D7 --> D8
    D8 --> C2
    C2 --> C3
    C3 --> C4

    C4 --> H1
    C4 --> H2
    H1 --> H3
    H3 --> D5
    H2 --> H4
    H4 --> H5
    H5 -- 否 --> D5
    H5 -- 是 --> H6
    H6 --> H7
    H7 --> H8
    H8 -- 成功 --> H3
    H8 -- 失败 --> H6

    J1 --> J2
    J2 --> J3
    J3 --> D5
    J1 --> J4
    J4 --> J5
```

## 逻辑概览

1. 服务启动后申请 etcd Lease，写入实例信息并持续 KeepAlive；服务停止后 Lease 到期，实例自动删除。
2. Resolver 首次解析服务时执行全量 Get，随后从查询 revision 的下一版本开始增量 Watch。
3. 每次请求通过 P2C 抽取两个健康实例，使用 `EWMA × (inflight + 1)` 评分并选择较低者。
4. 调用方通过 `Report` 回传请求结果和延迟，用于更新 EWMA、连续失败数、摘除及半开状态。
5. Janitor 统一清理超时未 Report 的请求，并回收长期未使用的服务缓存及 Watch。
