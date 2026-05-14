# grpc_requests 包说明

本目录保存 gRPC 手工测试请求文件。

- `task_scheduler.grpc`：用于支持 `.grpc` 请求文件的 IDE 或插件，例如 JetBrains HTTP Client。它可以直接调用调度器的健康检查、提交任务、查询任务、列出任务和列出 Worker 接口。

该目录不是 Python 包，而是课堂演示辅助目录。它的作用类似 REST 项目中的 `.http` 文件：把常用 RPC 调用样例保存下来，方便展示系统接口。
