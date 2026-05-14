"""WorkerService gRPC 实现。

当前系统中 Worker 主要通过“主动拉取”方式与调度器交互，但 Worker 仍然暴露一个
轻量 gRPC 服务，体现分布式系统中每个节点都可以拥有自己的服务接口。
"""

from __future__ import annotations

import grpc

from distributed_scheduler.generated import task_scheduler_pb2, task_scheduler_pb2_grpc


class WorkerService(task_scheduler_pb2_grpc.WorkerServiceServicer):
    """Worker 健康检查服务。"""

    def __init__(self, worker_id: str) -> None:
        """保存 Worker ID，便于健康检查响应标识节点。"""

        self._worker_id = worker_id

    def HealthCheck(
        self,
        request: task_scheduler_pb2.Empty,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.HealthCheckResponse:
        """返回 Worker 健康状态。"""

        return task_scheduler_pb2.HealthCheckResponse(
            ok=True,
            service=f"worker/{self._worker_id}",
            message="WorkerService 正常运行。",
        )
