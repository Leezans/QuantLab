## 分层职责

### `core`
- 只放跨模块共享的抽象契约、事件基类、任务基础类型。
- 不包含任何量化业务逻辑，也不关心具体存储、队列或外部接口实现。

### `infra`
- 提供 `EventBus`、`JobRepository`、`JobQueue`、`WorkerPool` 的技术实现。
- 当前默认实现是内存版总线、内存版任务仓储、内存版队列，以及线程/进程混合 worker。
- 负责“怎么运行”，不负责“为什么做这个业务任务”。

### `app`
- 负责把 `domain`、`core`、`infra` 装配成可运行系统。
- 包含 `JobService`、`AsyncTaskRuntime`、`EventToJobBridge` 等编排逻辑。
- 决定哪些事件要转成 Job、使用哪种执行模式、如何启动 worker。

## 异步任务系统

系统将“通知”和“重任务执行”拆开：

1. 模块先通过 `EventBus` 发布业务事件。
2. `app` 层的 `EventToJobBridge` 监听这些事件，并转换为标准化 `JobSpec`。
3. `JobService` 负责去重、创建 `JobRecord`、写入 `JobRepository`、提交到 `JobQueue`。
4. `HybridWorkerPool` 从队列取任务，根据 `execution_mode` 路由到线程池或进程池。
5. 任务状态变化继续通过 `EventBus` 发布 `job.queued / started / progressed / succeeded / failed`。
6. 前端或 API 适配层可以轮询 `JobService.get_status()`，也可以订阅任务事件做 WebSocket 推送。

## 当前代码入口

- `src/quantlab/app/bootstrap.py`
  - `build_async_task_runtime(settings)`：构建完整任务运行时。
- `src/quantlab/app/runtime.py`
  - 统一暴露 `submit_job / get_job_status / publish / subscribe / start / stop`。
- `src/quantlab/app/job_bindings.py`
  - 定义“事件 -> JobSpec”的默认映射。
- `src/quantlab/infra/workers/hybrid_pool.py`
  - 线程池与进程池混合调度入口。
