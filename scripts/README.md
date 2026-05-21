# scripts 包说明
本目录保存项目辅助脚本。

- `generate_grpc.py`：根据 `proto/task_scheduler.proto` 生成 Python gRPC 文件。
- `start_demo_cluster.py`：启动本地可视化集群控制台，在浏览器中配置参数并实时查看 Worker 和 Task 状态。

运行 `generate_grpc.py`：
```bash
python scripts/generate_grpc.py
```

生成后的文件会进入 `distributed_scheduler/generated/`。如果修改了 `.proto` 文件，必须重新运行该脚本。

启动可视化集群：
```bash
python scripts/start_demo_cluster.py
```

默认会打开本地 dashboard，例如 `http://127.0.0.1:8765/`。在界面里可以设置 Scheduler 地址、负载均衡策略、Worker 数量与并发、Task 模板与数量，然后启动集群并实时刷新状态。
