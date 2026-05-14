"""gRPC SchedulerService 实现。

本模块负责把网络层的 protobuf 请求转换为调度器内部操作。它本身尽量不保存状态，
真正的任务队列和 Worker 表由 ``TaskStore`` 维护。
"""

from __future__ import annotations

import logging

import grpc

from distributed_scheduler.generated import task_scheduler_pb2, task_scheduler_pb2_grpc
from distributed_scheduler.scheduler.task_store import TaskStore


class SchedulerService(task_scheduler_pb2_grpc.SchedulerServiceServicer):
    """调度器 gRPC 服务实现类。"""

    def __init__(self, store: TaskStore) -> None:
        """保存任务存储对象引用。"""

        self._store = store

    def SubmitTask(
        self,
        request: task_scheduler_pb2.SubmitTaskRequest,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.SubmitTaskResponse:
        """接收客户端提交的新任务。"""

        if request.task_type == task_scheduler_pb2.TASK_TYPE_UNSPECIFIED:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "必须指定 task_type。")

        task = self._store.submit_task(request)
        return task_scheduler_pb2.SubmitTaskResponse(
            task_id=task.task_id,
            message="任务已提交，等待 Worker 拉取执行。",
        )

    def QueryTask(
        self,
        request: task_scheduler_pb2.QueryTaskRequest,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.TaskInfo:
        """按 task_id 查询任务详情。"""

        task = self._store.get_task(request.task_id)
        if task is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"任务不存在：{request.task_id}")
        return task.to_proto()

    def ListTasks(
        self,
        request: task_scheduler_pb2.ListTasksRequest,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.ListTasksResponse:
        """返回最近任务列表。"""

        tasks = [task.to_proto() for task in self._store.list_tasks(request.limit)]
        return task_scheduler_pb2.ListTasksResponse(tasks=tasks)

    def WorkerHeartbeat(
        self,
        request: task_scheduler_pb2.WorkerHeartbeatRequest,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.WorkerHeartbeatResponse:
        """接收 Worker 心跳并更新节点状态。"""

        if not request.worker_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "worker_id 不能为空。")

        self._store.upsert_worker(request)
        return task_scheduler_pb2.WorkerHeartbeatResponse(
            accepted=True,
            message="心跳已接收。",
        )

    def PullTask(
        self,
        request: task_scheduler_pb2.PullTaskRequest,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.PullTaskResponse:
        """响应 Worker 的拉取任务请求。"""

        if not request.worker_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "worker_id 不能为空。")

        task = self._store.pull_task_for_worker(request.worker_id)
        if task is None:
            return task_scheduler_pb2.PullTaskResponse(
                has_task=False,
                message="暂无可分配任务，或当前 Worker 不是最优候选。",
            )

        return task_scheduler_pb2.PullTaskResponse(
            has_task=True,
            task=task.to_proto(),
            message="任务已分配。",
        )

    def ReportTaskResult(
        self,
        request: task_scheduler_pb2.ReportTaskResultRequest,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.ReportTaskResultResponse:
        """接收 Worker 回传的任务执行结果。"""

        accepted, message = self._store.report_task_result(
            worker_id=request.worker_id,
            task_id=request.task_id,
            success=request.success,
            result=request.result,
            error=request.error,
            finished_at_unix_ms=request.finished_at_unix_ms,
        )

        if not accepted:
            logging.warning("拒绝任务结果 task_id=%s reason=%s", request.task_id, message)

        return task_scheduler_pb2.ReportTaskResultResponse(
            accepted=accepted,
            message=message,
        )

    def ListWorkers(
        self,
        request: task_scheduler_pb2.Empty,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.ListWorkersResponse:
        """返回 Worker 状态列表。"""

        workers = [worker.to_proto(alive=alive) for worker, alive in self._store.list_workers()]
        return task_scheduler_pb2.ListWorkersResponse(workers=workers)

    def HealthCheck(
        self,
        request: task_scheduler_pb2.Empty,
        context: grpc.ServicerContext,
    ) -> task_scheduler_pb2.HealthCheckResponse:
        """调度器健康检查。"""

        return task_scheduler_pb2.HealthCheckResponse(
            ok=True,
            service="scheduler",
            message="SchedulerService 正常运行。",
        )
