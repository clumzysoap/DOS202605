# client 包说明

`client` 包实现命令行客户端，用于和调度器交互。

客户端职责：

- 提交任务到调度器。
- 查询单个任务状态。
- 列出最近任务。
- 查看当前已注册 Worker 和负载信息。
- 检查调度器健康状态。

主要文件：

- `cli.py`：命令行入口。

使用示例：

```bash
python -m distributed_scheduler.client.cli health
python -m distributed_scheduler.client.cli submit --type sleep --payload 3 --name demo-sleep
python -m distributed_scheduler.client.cli submit --type fibonacci --payload 30
python -m distributed_scheduler.client.cli submit --type word_count --payload "hello grpc distributed system"
python -m distributed_scheduler.client.cli tasks
python -m distributed_scheduler.client.cli workers
python -m distributed_scheduler.client.cli query --task-id <任务ID>
```
