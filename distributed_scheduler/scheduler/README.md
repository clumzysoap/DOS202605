# scheduler 包说明

`scheduler` 包实现系统中的中心调度器。

调度器职责：

- 接收客户端提交的任务。
- 保存任务状态和执行结果。
- 接收 Worker 心跳，维护 Worker 的 CPU、内存、并发数等状态。
- 根据配置选择负载均衡策略。
- 响应 Worker 的拉取任务请求，把任务分配给合适节点。
- 检查长时间未完成的任务，并重新放回队列。

主要文件：

- `task_store.py`：线程安全的任务表、队列和 Worker 状态管理。
- `load_balancer.py`：轮询、最少负载、综合加权三种策略。
- `service.py`：gRPC `SchedulerService` 的具体实现。
- `server.py`：调度器进程入口。

启动命令：

```bash
python -m distributed_scheduler.scheduler.server
```
