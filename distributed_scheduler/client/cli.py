"""命令行客户端。

运行方式：

    python -m distributed_scheduler.client.cli <子命令>

子命令包括 submit、query、tasks、workers 和 health。
"""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

import grpc

from distributed_scheduler.common.logging_utils import configure_logging
from distributed_scheduler.common.models import (
    task_status_to_text,
    task_type_from_cli,
    task_type_to_text,
)
from distributed_scheduler.generated import task_scheduler_pb2, task_scheduler_pb2_grpc


DEFAULT_SCHEDULER_ADDRESS = "127.0.0.1:50051"


def build_arg_parser() -> argparse.ArgumentParser:
    """创建客户端命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="gRPC 分布式任务调度系统客户端。")
    parser.add_argument(
        "--scheduler-address",
        default=DEFAULT_SCHEDULER_ADDRESS,
        help=f"调度器地址，默认 {DEFAULT_SCHEDULER_ADDRESS}。",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    submit_parser = subparsers.add_parser("submit", help="提交任务。")
    submit_parser.add_argument("--type", required=True, help="任务类型：sleep、fibonacci、word_count。")
    submit_parser.add_argument("--payload", required=True, help="任务参数。")
    submit_parser.add_argument("--name", default="", help="任务名称，不填则由调度器生成。")
    submit_parser.add_argument("--priority", type=int, default=0, help="任务优先级，当前版本预留。")

    query_parser = subparsers.add_parser("query", help="查询单个任务。")
    query_parser.add_argument("--task-id", required=True, help="任务 ID。")

    tasks_parser = subparsers.add_parser("tasks", help="列出最近任务。")
    tasks_parser.add_argument("--limit", type=int, default=20, help="最多返回多少条任务。")

    subparsers.add_parser("workers", help="列出 Worker 状态。")
    subparsers.add_parser("health", help="检查调度器健康状态。")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """客户端入口函数。

    Args:
        argv: 测试时可以传入自定义参数；正常命令行运行时使用 ``sys.argv``。

    Returns:
        进程退出码。0 表示成功，非 0 表示失败。
    """

    configure_logging("client")
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    channel = grpc.insecure_channel(args.scheduler_address)
    stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)

    try:
        if args.command == "submit":
            return _submit_task(stub, args)
        if args.command == "query":
            return _query_task(stub, args.task_id)
        if args.command == "tasks":
            return _list_tasks(stub, args.limit)
        if args.command == "workers":
            return _list_workers(stub)
        if args.command == "health":
            return _health_check(stub)
    except grpc.RpcError as exc:
        print(f"RPC 调用失败：{exc.details() or exc.code()}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"参数错误：{exc}", file=sys.stderr)
        return 2
    finally:
        channel.close()

    parser.print_help()
    return 1


def _submit_task(stub: task_scheduler_pb2_grpc.SchedulerServiceStub, args: argparse.Namespace) -> int:
    """提交任务并打印调度器返回的任务 ID。"""

    request = task_scheduler_pb2.SubmitTaskRequest(
        name=args.name,
        task_type=task_type_from_cli(args.type),
        payload=args.payload,
        priority=args.  priority,
    )
    response = stub.SubmitTask(request, timeout=5)
    print(f"任务已提交：task_id={response.task_id}")
    print(response.message)
    return 0


def _query_task(stub: task_scheduler_pb2_grpc.SchedulerServiceStub, task_id: str) -> int:
    """查询单个任务并打印详细信息。"""

    task = stub.QueryTask(task_scheduler_pb2.QueryTaskRequest(task_id=task_id), timeout=5)
    _print_task(task)
    return 0


def _list_tasks(stub: task_scheduler_pb2_grpc.SchedulerServiceStub, limit: int) -> int:
    """列出最近任务。"""

    response = stub.ListTasks(task_scheduler_pb2.ListTasksRequest(limit=limit), timeout=5)
    if not response.tasks:
        print("暂无任务。")
        return 0

    for task in response.tasks:
        _print_task(task)
        print("-" * 80)
    return 0


def _list_workers(stub: task_scheduler_pb2_grpc.SchedulerServiceStub) -> int:
    """列出 Worker 状态。"""

    response = stub.ListWorkers(task_scheduler_pb2.Empty(), timeout=5)
    if not response.workers:
        print("暂无 Worker 注册。")
        return 0

    for worker in response.workers:
        alive_text = "alive" if worker.alive else "dead"
        print(
            "worker_id={worker_id} address={address} status={status} "
            "running={running}/{capacity} cpu={cpu:.1f}% mem={mem:.1f}% last_heartbeat={heartbeat}".format(
                worker_id=worker.worker_id,
                address=worker.address,
                status=alive_text,
                running=worker.running_tasks,
                capacity=worker.max_concurrent_tasks,
                cpu=worker.cpu_percent,
                mem=worker.memory_percent,
                heartbeat=worker.last_heartbeat_unix_ms,
            )
        )
    return 0


def _health_check(stub: task_scheduler_pb2_grpc.SchedulerServiceStub) -> int:
    """检查调度器健康状态。"""

    response = stub.HealthCheck(task_scheduler_pb2.Empty(), timeout=5)
    print(f"ok={response.ok} service={response.service} message={response.message}")
    return 0 if response.ok else 1


def _print_task(task: task_scheduler_pb2.TaskInfo) -> None:
    """以多行格式打印任务详情。"""

    print(f"task_id: {task.task_id}")
    print(f"name: {task.name}")
    print(f"type: {task_type_to_text(task.task_type)}")
    print(f"status: {task_status_to_text(task.status)}")
    print(f"assigned_worker_id: {task.assigned_worker_id or '-'}")
    print(f"payload: {task.payload}")
    print(f"result: {task.result or '-'}")
    print(f"error: {task.error or '-'}")
    print(f"created_at_unix_ms: {task.created_at_unix_ms}")
    print(f"started_at_unix_ms: {task.started_at_unix_ms}")
    print(f"finished_at_unix_ms: {task.finished_at_unix_ms}")


if __name__ == "__main__":
    raise SystemExit(main())
