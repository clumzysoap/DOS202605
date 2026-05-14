# distributed_scheduler 包说明

`distributed_scheduler` 是课程项目的主 Python 包，包含分布式任务调度与负载均衡系统的全部业务代码。

子包职责：

- `common/`：公共工具、配置加载、日志初始化、任务数据模型。
- `generated/`：由 `.proto` 文件生成的 gRPC Python 代码。
- `scheduler/`：调度器服务端，负责接收任务、维护 Worker 状态、选择合适 Worker、保存任务结果。
- `worker/`：Worker 节点，负责向调度器发送心跳、拉取任务、执行任务、回传结果。
- `client/`：命令行客户端，用于提交任务、查询任务、列出任务和 Worker。

推荐启动顺序：

1. 安装依赖：`pip install -r requirements.txt`
2. 生成 gRPC 代码：`python scripts/generate_grpc.py`
3. 启动调度器：`python -m distributed_scheduler.scheduler.server`
4. 启动一个或多个 Worker：`python -m distributed_scheduler.worker.worker_node --worker-id worker-1`
5. 使用客户端提交任务：`python -m distributed_scheduler.client.cli submit --type fibonacci --payload 30`
