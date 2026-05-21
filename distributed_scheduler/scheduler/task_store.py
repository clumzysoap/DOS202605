"""调度器的线程安全内存状态管理。

gRPC Python 服务端使用线程池处理请求，因此同一时刻可能有多个线程同时提交任务、
Worker 心跳、Worker 拉取任务或提交结果。本模块用 ``threading.RLock`` 保护共享状态。

为了课程项目保持清晰，本实现把状态保存在内存中。真实系统可以把本模块替换为
Redis、数据库或持久化队列，而不改变 gRPC 接口。
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections import deque

from distributed_scheduler.common.models import InternalTaskStatus, TaskRecord, WorkerRecord
from distributed_scheduler.common.time_utils import now_unix_ms, seconds_since
from distributed_scheduler.generated import task_scheduler_pb2
from distributed_scheduler.scheduler.load_balancer import LoadBalancer


class TaskStore:
    """保存任务、队列和 Worker 状态的核心类。"""

    def __init__(
        self,
        strategy: str,
        worker_ttl_seconds: int,
        task_timeout_seconds: int,
    ) -> None:
        """初始化任务存储。

        Args:
            strategy: 负载均衡策略名。
            worker_ttl_seconds: Worker 心跳失效阈值。
            task_timeout_seconds: 任务运行超时时间。
        """

        self._lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}
        self._workers: dict[str, WorkerRecord] = {}
        self._queued_task_ids: deque[str] = deque()
        self._load_balancer = LoadBalancer(strategy=strategy)
        self._worker_ttl_seconds = worker_ttl_seconds
        self._task_timeout_seconds = task_timeout_seconds

    def submit_task(self, request: task_scheduler_pb2.SubmitTaskRequest) -> TaskRecord:
        """创建任务并加入待调度队列。"""

        with self._lock:
            task_id = uuid.uuid4().hex
            task = TaskRecord(
                task_id=task_id,
                name=request.name or f"task-{task_id[:8]}",
                task_type=request.task_type,
                payload=request.payload,
                priority=request.priority,
                status=InternalTaskStatus.QUEUED,
                created_at_unix_ms=now_unix_ms(),
            )
            self._tasks[task_id] = task
            self._queued_task_ids.append(task_id)
            logging.info("任务已入队 task_id=%s name=%s", task.task_id, task.name)
            return task

    def get_task(self, task_id: str) -> TaskRecord | None:
        """按 ID 查询任务。"""

        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self, limit: int) -> list[TaskRecord]:
        """列出最近创建的任务。"""

        with self._lock:
            tasks = sorted(
                self._tasks.values(),
                key=lambda task: task.created_at_unix_ms,
                reverse=True,
            )
            if limit > 0:
                return tasks[:limit]
            return tasks

    def upsert_worker(self, heartbeat: task_scheduler_pb2.WorkerHeartbeatRequest) -> WorkerRecord:
        """根据心跳新增或更新 Worker 状态。"""

        with self._lock:
            worker = self._workers.get(heartbeat.worker_id)
            if worker is None:
                worker = WorkerRecord(
                    worker_id=heartbeat.worker_id,
                    address=heartbeat.address,
                    max_concurrent_tasks=max(heartbeat.max_concurrent_tasks, 1),
                    running_tasks=max(heartbeat.running_tasks, 0),
                    cpu_percent=heartbeat.cpu_percent,
                    memory_percent=heartbeat.memory_percent,
                    last_heartbeat_unix_ms=heartbeat.timestamp_unix_ms or now_unix_ms(),
                )
                self._workers[worker.worker_id] = worker
                logging.info("注册新 Worker worker_id=%s address=%s", worker.worker_id, worker.address)
                return worker

            worker.address = heartbeat.address
            worker.max_concurrent_tasks = max(heartbeat.max_concurrent_tasks, 1)
            worker.running_tasks = max(heartbeat.running_tasks, 0)
            worker.cpu_percent = heartbeat.cpu_percent
            worker.memory_percent = heartbeat.memory_percent
            worker.last_heartbeat_unix_ms = heartbeat.timestamp_unix_ms or now_unix_ms()
            return worker

    def list_workers(self) -> list[tuple[WorkerRecord, bool]]:
        """返回所有 Worker 以及是否存活。"""

        with self._lock:
            return [(worker, self._is_worker_alive(worker)) for worker in self._workers.values()]

    def pull_task_for_worker(self, worker_id: str) -> TaskRecord | None:
        """为指定 Worker 分配一个任务。

        Worker 主动调用 PullTask。调度器仍然会执行一次全局负载均衡判断：
        只有当调用者正好是当前策略选出的最佳 Worker 时，才真正分配任务。
        这样可以保留“Worker 主动拉取”的简单网络模型，同时展示中心调度器的策略决策。
        """

        with self._lock:
            self.requeue_timed_out_tasks_locked()

            worker = self._workers.get(worker_id)
            if worker is None:
                logging.warning("未知 Worker 尝试拉取任务 worker_id=%s", worker_id)
                return None

            if not self._is_worker_alive(worker) or not worker.has_capacity():
                return None

            next_task = self._get_next_queued_task_locked()
            if next_task is None : return None

            candidates = [
                item
                for item in self._workers.values()
                if self._is_worker_alive(item) and item.has_capacity()
            ]
            chosen_worker = self._load_balancer.choose_workers(candidates,next_task,worker_id)
            if chosen_worker is None or chosen_worker.worker_id != worker_id:
                return None

            task = self._pop_next_queued_task_locked()
            if task is None:
                return None

            task.status = InternalTaskStatus.RUNNING
            task.assigned_worker_id = worker_id
            task.started_at_unix_ms = now_unix_ms()
            task.error = ""

            worker.running_tasks += 1
            worker.active_task_ids.add(task.task_id)

            logging.info(
                "任务已分配 task_id=%s worker_id=%s strategy=%s",
                task.task_id,
                worker_id,
                self._load_balancer.strategy,
            )
            return task

    def report_task_result(
        self,
        worker_id: str,
        task_id: str,
        success: bool,
        result: str,
        error: str,
        finished_at_unix_ms: int,
    ) -> tuple[bool, str]:
        """保存 Worker 回传的任务执行结果。"""

        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False, f"任务不存在：{task_id}"

            if task.assigned_worker_id != worker_id:
                return False, f"任务 {task_id} 当前不属于 Worker {worker_id}"

            if task.status not in {InternalTaskStatus.RUNNING, InternalTaskStatus.TIMED_OUT}:
                return False, f"任务 {task_id} 当前状态不允许提交结果：{task.status.value}"

            task.status = InternalTaskStatus.SUCCEEDED if success else InternalTaskStatus.FAILED
            task.result = result
            task.error = error
            task.finished_at_unix_ms = finished_at_unix_ms or now_unix_ms()

            worker = self._workers.get(worker_id)
            if worker is not None:
                worker.active_task_ids.discard(task_id)
                worker.running_tasks = max(worker.running_tasks - 1, 0)

            logging.info(
                "任务结果已保存 task_id=%s worker_id=%s success=%s",
                task_id,
                worker_id,
                success,
            )
            return True, "任务结果已保存"

    def requeue_timed_out_tasks(self) -> int:
        """把运行超时的任务重新放回队列。"""

        with self._lock:
            return self.requeue_timed_out_tasks_locked()

    def requeue_timed_out_tasks_locked(self) -> int:
        """在持有锁的情况下执行超时任务回收。

        这个方法被 ``pull_task_for_worker`` 内部调用，所以单独拆出 locked 版本，
        避免重复获取锁造成逻辑不清晰。
        """

        requeued_count = 0
        for task in self._tasks.values():
            if task.status != InternalTaskStatus.RUNNING:
                continue

            if seconds_since(task.started_at_unix_ms) <= self._task_timeout_seconds:
                continue

            old_worker_id = task.assigned_worker_id
            task.status = InternalTaskStatus.QUEUED
            task.assigned_worker_id = ""
            task.error = f"任务在 Worker {old_worker_id} 上执行超时，已重新入队。"
            task.started_at_unix_ms = 0

            if task.task_id not in self._queued_task_ids:
                self._queued_task_ids.append(task.task_id)

            old_worker = self._workers.get(old_worker_id)
            if old_worker is not None:
                old_worker.active_task_ids.discard(task.task_id)
                old_worker.running_tasks = max(old_worker.running_tasks - 1, 0)

            requeued_count += 1
            logging.warning("任务执行超时并重新入队 task_id=%s old_worker=%s", task.task_id, old_worker_id)

        return requeued_count

    def _pop_next_queued_task_locked(self) -> TaskRecord | None:
        """从队列中弹出下一个仍处于 QUEUED 状态的任务。"""

        while self._queued_task_ids:
            task_id = self._queued_task_ids.popleft()
            task = self._tasks.get(task_id)
            if task is None:
                continue
            if task.status == InternalTaskStatus.QUEUED:
                return task
        return None

    def _get_next_queued_task_locked(self) -> TaskRecord | None:
        """遍历队列寻找下一个 QUEUED 状态任务，不对队列做任何修改。"""

        # 直接遍历 deque
        for task_id in self._queued_task_ids:
            task = self._tasks.get(task_id)
            if task is not None and task.status == InternalTaskStatus.QUEUED:
                return task  # 找到即返回

        return None

    def _is_worker_alive(self, worker: WorkerRecord) -> bool:
        """判断 Worker 是否仍被调度器视为存活。"""

        return seconds_since(worker.last_heartbeat_unix_ms) <= self._worker_ttl_seconds
