# Task Cancellation Architecture

## 组件关系图 / Component Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                           Server                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Subscriber  │  │  Processor   │  │  Cancellation Loop   │  │
│  │              │  │              │  │                      │  │
│  │  - Receives  │  │  - Runs      │  │  - Receives events  │  │
│  │    cancel    │  │    tasks     │  │  - Calls cancel()   │  │
│  │    events    │  │              │  │                      │  │
│  │              │  │  ┌────────┐  │  │                      │  │
│  │  event_rx ───┼──┼──┤Cancels │  │  │                      │  │
│  │              │  │  │        │  │  │                      │  │
│  │              │  │  └────────┘  │  │                      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│         │                  │                      │              │
│         │                  │                      │              │
└─────────┼──────────────────┼──────────────────────┼──────────────┘
          │                  │                      │
          ▼                  │                      │
    ┌─────────┐              │                      │
    │  Redis  │◄─────────────┘                      │
    │ Pub/Sub │                                      │
    └─────────┘                                      │
          ▲                                          │
          │                                          │
          │  publish_cancelation()                   │
    ┌─────────────┐                                  │
    │  Inspector  │──────────────────────────────────┘
    └─────────────┘        cancel_task()
```

## 数据流图 / Data Flow Diagram

```
1. 用户调用取消 / User Initiates Cancellation
   ┌──────────┐
   │Inspector │ cancel_task(task_id)
   └────┬─────┘
        │
        ▼
   ┌──────────────────────┐
   │Broker.publish_       │
   │cancelation(task_id)  │
   └────┬─────────────────┘
        │
        ▼
2. Redis 发布事件 / Redis Publishes Event
   ┌──────────────────────┐
   │ Redis Pub/Sub        │
   │ Channel: asynq:cancel│
   │ Message: task_id     │
   └────┬─────────────────┘
        │
        ▼
3. Subscriber 接收 / Subscriber Receives
   ┌──────────────────────┐
   │Subscriber.start()    │
   │ - cancelation_pub_sub│
   │ - Receives task_id   │
   └────┬─────────────────┘
        │
        ▼
   ┌──────────────────────┐
   │event_tx.send(        │
   │TaskCancelled{task_id}│
   └────┬─────────────────┘
        │
        ▼
4. Server 处理事件 / Server Handles Event
   ┌──────────────────────┐
   │Cancellation Loop     │
   │ - event_rx.recv()    │
   │ - Extract task_id    │
   └────┬─────────────────┘
        │
        ▼
   ┌──────────────────────┐
   │cancelations.cancel(  │
   │task_id)              │
   └────┬─────────────────┘
        │
        ▼
5. 取消令牌触发 / Token Triggered
   ┌──────────────────────┐
   │CancellationToken     │
   │ .cancel()            │
   └────┬─────────────────┘
        │
        ▼
6. 任务中断 / Task Interrupted
   ┌──────────────────────┐
   │tokio::select! {      │
   │  result = task =>    │
   │  _ = timeout =>      │
   │  _ = token.cancelled │◄─── 这里被触发
   │}                     │     Triggered here
   └──────────────────────┘
```

## Cancellations 内部结构 / Cancellations Internal Structure

```
┌─────────────────────────────────────────────────────┐
│              Cancellations                           │
│                                                     │
│  ┌───────────────────────────────────────────────┐ │
│  │ Arc<Mutex<HashMap<String, CancellationToken>>>│ │
│  │                                               │ │
│  │  ┌─────────────────────────────────────────┐ │ │
│  │  │ "task_001" → CancellationToken { ... }  │ │ │
│  │  │ "task_002" → CancellationToken { ... }  │ │ │
│  │  │ "task_003" → CancellationToken { ... }  │ │ │
│  │  └─────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────┘ │
│                                                     │
│  Methods:                                          │
│  • add(task_id, token)      ── 注册               │
│  • remove(task_id)          ── 清理               │
│  • cancel(task_id) -> bool  ── 取消 (Go兼容)     │
│  • len() / is_empty()       ── 查询               │
└─────────────────────────────────────────────────────┘
```

## 任务生命周期 / Task Lifecycle

```
┌──────────┐
│ Enqueued │
└────┬─────┘
     │
     ▼
┌──────────┐     ┌──────────────────────────────┐
│ Dequeued │────►│ Create CancellationToken      │
└──────────┘     └─────┬────────────────────────┘
                       │
                       ▼
                ┌──────────────────────────────┐
                │ cancelations.add(id, token)  │
                └─────┬────────────────────────┘
                       │
                       ▼
                ┌──────────────────────────────┐
                │ Execute with tokio::select!  │
                │                              │
    ┌───────────┤  • Task completion           │
    │           │  • Timeout                   │
    │           │  • Cancellation ◄────────────┼─── cancel() 调用
    │           │                              │
    │           └─────┬────────────────────────┘
    │                 │
    │                 ▼
    │           ┌──────────────────────────────┐
    └──────────►│ cancelations.remove(id)      │
                └─────┬────────────────────────┘
                      │
                      ▼
                ┌──────────────────────────────┐
                │ Done / Retry / Archive       │
                └──────────────────────────────┘
```

## 性能特征 / Performance Characteristics

```
┌─────────────────────────────────────────────────────┐
│                   操作延迟 / Latency                │
├─────────────────────────────────────────────────────┤
│                                                     │
│  add/remove:           < 1μs  (Mutex lock)         │
│  cancel():             < 1μs  (Mutex lock + signal)│
│  Redis pub/sub:        1-5ms  (Network)            │
│  Total cancellation:   1-10ms (Typical)            │
│                                                     │
├─────────────────────────────────────────────────────┤
│                   内存使用 / Memory                 │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Per task overhead:    ~100 bytes                  │
│  • String (task_id):   ~24 + len bytes             │
│  • CancellationToken:  ~56 bytes                   │
│  • HashMap entry:      ~24 bytes                   │
│                                                     │
│  Example (1000 tasks): ~100KB                      │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## 关键设计决策 / Key Design Decisions

```
┌─────────────────────────────────────────────────────┐
│ 1. CancellationToken vs AtomicBool                 │
│    ✓ tokio 生态系统集成                             │
│    ✓ 支持 tokio::select!                            │
│    ✓ 可以安全克隆                                   │
│    ✓ 标准模式                                       │
├─────────────────────────────────────────────────────┤
│ 2. Arc<Mutex<HashMap>> vs DashMap                  │
│    ✓ 简单明了                                       │
│    ✓ 性能足够（取消不频繁）                         │
│    ✓ 标准库类型                                     │
│    ✓ 更少的依赖                                     │
├─────────────────────────────────────────────────────┤
│ 3. Server 中处理取消事件                            │
│    ✓ 保持 Processor 简洁                            │
│    ✓ 清晰的责任分离                                 │
│    ✓ 易于测试和维护                                 │
│    ✓ 灵活的扩展性                                   │
└─────────────────────────────────────────────────────┘
```

## 兼容性矩阵 / Compatibility Matrix

```
┌─────────────────────────┬──────────┬──────────┬──────┐
│ 特性 / Feature          │   Go     │   Rust   │ 兼容 │
├─────────────────────────┼──────────┼──────────┼──────┤
│ Cancellations 结构       │    ✓     │    ✓     │  ✅  │
│ Cancel(taskID) 方法     │    ✓     │    ✓     │  ✅  │
│ Redis pub/sub           │    ✓     │    ✓     │  ✅  │
│ 任务追踪                │    ✓     │    ✓     │  ✅  │
│ 取消传播                │ context  │  Token   │  ✅  │
│ 线程安全                │    ✓     │    ✓     │  ✅  │
│ 取消原因                │    ✓     │    ✗     │  🔜  │
│ 批量取消                │    ✗     │    ✗     │  🔜  │
└─────────────────────────┴──────────┴──────────┴──────┘
```