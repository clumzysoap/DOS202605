# proto 包说明

本目录保存 gRPC 的接口定义文件。

- `task_scheduler.proto` 是整个系统最重要的跨进程通信契约。
- 修改 `.proto` 后，需要运行 `python scripts/generate_grpc.py` 重新生成 Python 代码。
- 生成文件会放到 `distributed_scheduler/generated/`，业务代码只依赖生成后的 `task_scheduler_pb2.py` 和 `task_scheduler_pb2_grpc.py`。

在课程报告中，可以把本目录作为“接口设计”章节的主要材料，说明客户端、调度器和 Worker 之间的 RPC 调用关系。
