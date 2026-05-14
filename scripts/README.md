# scripts 包说明

本目录保存项目辅助脚本。

- `generate_grpc.py`：根据 `proto/task_scheduler.proto` 生成 Python gRPC 文件。

运行命令：

```bash
python scripts/generate_grpc.py
```

生成后的文件会进入 `distributed_scheduler/generated/`。如果修改了 `.proto` 文件，必须重新运行该脚本。
