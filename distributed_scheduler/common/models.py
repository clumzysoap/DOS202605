"""系统内部数据模型。

gRPC 生成的 ``*_pb2.py`` 消息对象适合网络传输，但不适合承载调度器内部状态。
例如调度器需要频繁修改任务状态、记录锁保护下的 Worker 信息、计算负载分数等。
因此本模块定义 Python dataclass，再提供与 protobuf 消息之间的转换函数。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from distributed_scheduler.generated import task_scheduler_pb2


class InternalTaskStatus(str, Enum):
    """调度器内部使用的任务状态枚举。"""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


@dataclass
class TaskRecord:
    """调度器内部保存的任务记录。

    字段基本对应 ``proto/task_scheduler.proto`` 中的 ``TaskInfo``，但是内部模型
    允许直接修改字段，适合放在内存字典中管理。
    """

    task_id: str
    name: str
    task_type: int
    payload: str
    priority: int
    status: InternalTaskStatus
    created_at_unix_ms: int
    assigned_worker_id: str = ""
    result: str = ""
    error: str = ""
    started_at_unix_ms: int = 0
    finished_at_unix_ms: int = 0

    def to_proto(self) -> task_scheduler_pb2.TaskInfo:
        """把内部任务记录转换为 protobuf 消息。"""

        return task_scheduler_pb2.TaskInfo(
            task_id=self.task_id,
            name=self.name,
            task_type=self.task_type,
            payload=self.payload,
            priority=self.priority,
            status=internal_status_to_proto(self.status),
            assigned_worker_id=self.assigned_worker_id,
            result=self.result,
            error=self.error,
            created_at_unix_ms=self.created_at_unix_ms,
            started_at_unix_ms=self.started_at_unix_ms,
            finished_at_unix_ms=self.finished_at_unix_ms,
        )


@dataclass
class WorkerRecord:
    """调度器内部保存的 Worker 状态。

    Worker 的 CPU、内存、正在运行任务数量都来自心跳上报。调度器通过这些字段计算
    负载分数，从而决定下一个任务分配给谁。
    """

    worker_id: str
    address: str
    max_concurrent_tasks: int
    running_tasks: int
    cpu_percent: float
    memory_percent: float
    last_heartbeat_unix_ms: int
    active_task_ids: set[str] = field(default_factory=set)

    def has_capacity(self) -> bool:
        """判断 Worker 是否还有可用并发槽位。"""

        return self.running_tasks < self.max_concurrent_tasks

    def load_score(self) -> float:
        """计算 Worker 的综合负载分数。

        分数越低代表越适合接收新任务。这里使用一个可解释的加权公式：

        - CPU 使用率权重 0.4
        - 内存使用率权重 0.3
        - 并发槽位占用率权重 0.3

        课程展示时可以在报告里说明：真实生产系统会根据业务瓶颈调整权重。
        """

        capacity = max(self.max_concurrent_tasks, 1)
        concurrency_ratio = self.running_tasks / capacity * 100.0
        return self.cpu_percent * 0.4 + self.memory_percent * 0.3 + concurrency_ratio * 0.3

    def to_proto(self, alive: bool) -> task_scheduler_pb2.WorkerInfo:
        """把内部 Worker 状态转换为 protobuf 消息。"""

        return task_scheduler_pb2.WorkerInfo(
            worker_id=self.worker_id,
            address=self.address,
            max_concurrent_tasks=self.max_concurrent_tasks,
            running_tasks=self.running_tasks,
            cpu_percent=self.cpu_percent,
            memory_percent=self.memory_percent,
            last_heartbeat_unix_ms=self.last_heartbeat_unix_ms,
            alive=alive,
        )


def internal_status_to_proto(status: InternalTaskStatus) -> int:
    """把内部任务状态转换为 protobuf 枚举值。"""

    mapping = {
        InternalTaskStatus.QUEUED: task_scheduler_pb2.TASK_STATUS_QUEUED,
        InternalTaskStatus.RUNNING: task_scheduler_pb2.TASK_STATUS_RUNNING,
        InternalTaskStatus.SUCCEEDED: task_scheduler_pb2.TASK_STATUS_SUCCEEDED,
        InternalTaskStatus.FAILED: task_scheduler_pb2.TASK_STATUS_FAILED,
        InternalTaskStatus.TIMED_OUT: task_scheduler_pb2.TASK_STATUS_TIMED_OUT,
    }
    return mapping[status]


def task_type_from_cli(value: str) -> int:
    """把命令行输入的任务类型转换成 protobuf 枚举值。

    Args:
        value: 用户输入的任务类型，例如 ``sleep``、``fibonacci`` 或 ``word_count``。

    Raises:
        ValueError: 任务类型不支持时抛出，客户端会把错误展示给用户。
    """

    normalized = value.strip().lower().replace("-", "_")
    mapping = {
        "sleep": task_scheduler_pb2.TASK_TYPE_SLEEP,
        "fibonacci": task_scheduler_pb2.TASK_TYPE_FIBONACCI,
        "fib": task_scheduler_pb2.TASK_TYPE_FIBONACCI,
        "word_count": task_scheduler_pb2.TASK_TYPE_WORD_COUNT,
        "wordcount": task_scheduler_pb2.TASK_TYPE_WORD_COUNT,
    }
    if normalized not in mapping:
        supported = ", ".join(sorted(mapping))
        raise ValueError(f"不支持的任务类型：{value}。可选类型：{supported}")
    return mapping[normalized]


def task_type_to_text(task_type: int) -> str:
    """把 protobuf 任务类型枚举转换成人类可读文本。"""

    mapping = {
        task_scheduler_pb2.TASK_TYPE_SLEEP: "sleep",
        task_scheduler_pb2.TASK_TYPE_FIBONACCI: "fibonacci",
        task_scheduler_pb2.TASK_TYPE_WORD_COUNT: "word_count",
    }
    return mapping.get(task_type, "unknown")


def task_status_to_text(status: int) -> str:
    """把 protobuf 任务状态枚举转换成人类可读文本。"""

    mapping = {
        task_scheduler_pb2.TASK_STATUS_QUEUED: "queued",
        task_scheduler_pb2.TASK_STATUS_RUNNING: "running",
        task_scheduler_pb2.TASK_STATUS_SUCCEEDED: "succeeded",
        task_scheduler_pb2.TASK_STATUS_FAILED: "failed",
        task_scheduler_pb2.TASK_STATUS_TIMED_OUT: "timed_out",
    }
    return mapping.get(status, "unknown")


def sort_tasks_for_display(tasks: Iterable[task_scheduler_pb2.TaskInfo]) -> list[task_scheduler_pb2.TaskInfo]:
    """按创建时间倒序排列任务，方便客户端展示最近任务。"""

    return sorted(tasks, key=lambda task: task.created_at_unix_ms, reverse=True)
