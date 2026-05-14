# worker 包说明

`worker` 包实现分布式系统中的工作节点。

Worker 职责：

- 定期向调度器发送心跳，报告 CPU、内存、正在执行任务数等状态。
- 主动向调度器拉取任务。
- 使用本地线程池执行任务。
- 将执行结果或错误信息回传给调度器。
- 暴露一个简单的健康检查 gRPC 服务，方便演示节点也是独立服务。

主要文件：

- `executor.py`：具体任务执行逻辑，目前支持 sleep、fibonacci、word_count。
- `service.py`：Worker 自己暴露的 `WorkerService` 健康检查。
- `worker_node.py`：Worker 进程入口，负责心跳、拉取任务和结果回传。

启动示例：

```bash
python -m distributed_scheduler.worker.worker_node --worker-id worker-1 --listen-port 50061
python -m distributed_scheduler.worker.worker_node --worker-id worker-2 --listen-port 50062
```
