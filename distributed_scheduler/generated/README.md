# generated 包说明

本目录用于保存由 `proto/task_scheduler.proto` 生成的 gRPC Python 文件。

正常情况下会生成：

- `task_scheduler_pb2.py`
- `task_scheduler_pb2_grpc.py`

这些文件由 `python scripts/generate_grpc.py` 自动生成，不建议手工修改。业务代码通过导入这些生成文件来创建 gRPC 服务端和客户端。

如果本目录暂时没有生成文件，请先在项目根目录执行：

```bash
python scripts/generate_grpc.py
```
