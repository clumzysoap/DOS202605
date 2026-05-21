"""启动课程演示集群并初始化任务队列。

默认行为：

1. 启动 1 个 Scheduler。
2. 等待 Scheduler 健康检查通过。
3. 提交 200 个 sleep / fibonacci / word_count 混合任务。
4. 启动 5 个 Worker 消费任务。
5. 前台保持运行，按 Ctrl+C 时关闭本脚本启动的子进程。

运行方式：

    python scripts/start_demo_cluster.py

常用参数：

    python scripts/start_demo_cluster.py --strategy round_robin
    python scripts/start_demo_cluster.py --tasks 200 --workers 5 --worker-concurrency 2
"""

from __future__ import annotations

import argparse
import itertools
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import grpc  # noqa: E402

from distributed_scheduler.generated import task_scheduler_pb2, task_scheduler_pb2_grpc  # noqa: E402


DEFAULT_SCHEDULER_HOST = "127.0.0.1"
DEFAULT_SCHEDULER_PORT = 50051
DEFAULT_WORKER_START_PORT = 50061
DEFAULT_TASK_COUNT = 200
DEFAULT_WORKER_COUNT = 5


def build_arg_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="启动 Scheduler + 多 Worker，并初始化演示任务队列。")
    parser.add_argument("--scheduler-host", default=DEFAULT_SCHEDULER_HOST, help="Scheduler 监听地址。")
    parser.add_argument("--scheduler-port", type=int, default=DEFAULT_SCHEDULER_PORT, help="Scheduler 监听端口。")
    parser.add_argument(
        "--strategy",
        default="weighted_score",
        choices=["round_robin", "least_loaded", "weighted_score"],
        help="Scheduler 负载均衡策略。",
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKER_COUNT, help="启动 Worker 数量。")
    parser.add_argument("--worker-start-port", type=int, default=DEFAULT_WORKER_START_PORT, help="Worker 起始监听端口。")
    parser.add_argument("--worker-concurrency", type=int, default=2, help="每个 Worker 的最大并发任务数。")
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASK_COUNT, help="初始化提交的任务数量。")
    parser.add_argument(
        "--start-workers-first",
        action="store_true",
        help="先启动 Worker 再提交任务。默认先提交任务再启动 Worker，以便形成初始队列。",
    )
    parser.add_argument(
        "--use-existing-scheduler",
        action="store_true",
        help="连接已存在的 Scheduler，不再启动新的 Scheduler 进程。",
    )
    return parser


def main() -> int:
    """脚本入口。"""

    args = build_arg_parser().parse_args()
    scheduler_address = f"{args.scheduler_host}:{args.scheduler_port}"
    log_dir = PROJECT_ROOT / "logs" / "demo_cluster"
    log_dir.mkdir(parents=True, exist_ok=True)

    processes: list[subprocess.Popen[bytes]] = []
    try:
        if args.use_existing_scheduler:
            wait_for_scheduler(scheduler_address)
            print(f"使用已存在的 Scheduler：{scheduler_address}")
        else:
            ensure_scheduler_not_running(scheduler_address)
            scheduler = start_scheduler(args.scheduler_host, args.scheduler_port, args.strategy, log_dir)
            processes.append(scheduler)
            wait_for_scheduler(scheduler_address)
            print(f"Scheduler 已启动：{scheduler_address} strategy={args.strategy}")

        if args.start_workers_first:
            processes.extend(start_workers(args, scheduler_address, log_dir))
            wait_for_workers(scheduler_address, expected_workers=args.workers)
            print(f"{args.workers} 个 Worker 已启动。")
            submit_demo_tasks(scheduler_address, args.tasks)
        else:
            submit_demo_tasks(scheduler_address, args.tasks)
            print(f"已提交 {args.tasks} 个演示任务。")
            processes.extend(start_workers(args, scheduler_address, log_dir))
            wait_for_workers(scheduler_address, expected_workers=args.workers)
            print(f"{args.workers} 个 Worker 已启动。")

        print("演示集群正在运行。按 Ctrl+C 停止本脚本启动的进程。")
        while True:
            exited = [process for process in processes if process.poll() is not None]
            if exited:
                for process in exited:
                    print(f"子进程已退出 pid={process.pid} exit_code={process.returncode}")
                return 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n收到停止信号，正在关闭演示集群...")
        return 0
    finally:
        stop_processes(processes)


def start_scheduler(host: str, port: int, strategy: str, log_dir: Path) -> subprocess.Popen[bytes]:
    """启动 Scheduler 子进程。"""

    log_file = (log_dir / "scheduler.log").open("ab")
    command = [
        sys.executable,
        "-m",
        "distributed_scheduler.scheduler.server",
        "--host",
        host,
        "--port",
        str(port),
        "--strategy",
        strategy,
    ]
    return subprocess.Popen(command, cwd=PROJECT_ROOT, stdout=log_file, stderr=subprocess.STDOUT)


def start_workers(
    args: argparse.Namespace,
    scheduler_address: str,
    log_dir: Path,
) -> list[subprocess.Popen[bytes]]:
    """启动多个 Worker 子进程。"""

    processes: list[subprocess.Popen[bytes]] = []
    for index in range(1, args.workers + 1):
        worker_id = f"worker-{index}"
        listen_port = args.worker_start_port + index - 1
        log_file = (log_dir / f"{worker_id}.log").open("ab")
        command = [
            sys.executable,
            "-m",
            "distributed_scheduler.worker.worker_node",
            "--worker-id",
            worker_id,
            "--scheduler-address",
            scheduler_address,
            "--listen-host",
            "127.0.0.1",
            "--listen-port",
            str(listen_port),
            "--max-concurrent-tasks",
            str(args.worker_concurrency),
        ]
        process = subprocess.Popen(command, cwd=PROJECT_ROOT, stdout=log_file, stderr=subprocess.STDOUT)
        processes.append(process)
        print(f"Worker 启动中：{worker_id} listen_port={listen_port} pid={process.pid}")
    return processes


def submit_demo_tasks(scheduler_address: str, task_count: int) -> None:
    """通过 gRPC 直接提交混合任务。"""

    channel = grpc.insecure_channel(scheduler_address)
    stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
    task_factories = itertools.cycle(
        [
            build_sleep_task,
            build_fibonacci_task,
            build_word_count_task,
        ]
    )

    try:
        for index in range(1, task_count + 1):
            request = next(task_factories)(index)
            stub.SubmitTask(request, timeout=5)
            if index % 25 == 0 or index == task_count:
                print(f"任务提交进度：{index}/{task_count}")
    finally:
        channel.close()


def build_sleep_task(index: int) -> task_scheduler_pb2.SubmitTaskRequest:
    """构造 sleep 任务。"""

    seconds = 1 + index % 5
    return task_scheduler_pb2.SubmitTaskRequest(
        name=f"demo-sleep-{index:03d}",
        task_type=task_scheduler_pb2.TASK_TYPE_SLEEP,
        payload=str(seconds),
        priority=index % 10,
    )


def build_fibonacci_task(index: int) -> task_scheduler_pb2.SubmitTaskRequest:
    """构造 Fibonacci 任务。"""

    n = 28 + index % 8
    return task_scheduler_pb2.SubmitTaskRequest(
        name=f"demo-fibonacci-{index:03d}",
        task_type=task_scheduler_pb2.TASK_TYPE_FIBONACCI,
        payload=str(n),
        priority=index % 10,
    )


def build_word_count_task(index: int) -> task_scheduler_pb2.SubmitTaskRequest:
    """构造 word_count 任务。"""

    payload = (
        "distributed scheduler load balancing demo "
        f"task {index} worker queue grpc priority"
    )
    return task_scheduler_pb2.SubmitTaskRequest(
        name=f"demo-word-count-{index:03d}",
        task_type=task_scheduler_pb2.TASK_TYPE_WORD_COUNT,
        payload=payload,
        priority=index % 10,
    )


def wait_for_scheduler(scheduler_address: str, timeout_seconds: int = 15) -> None:
    """等待 Scheduler 健康检查通过。"""

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            channel = grpc.insecure_channel(scheduler_address)
            stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
            response = stub.HealthCheck(task_scheduler_pb2.Empty(), timeout=1)
            channel.close()
            if response.ok:
                return
        except grpc.RpcError:
            time.sleep(0.5)
    raise RuntimeError(f"Scheduler 未在 {timeout_seconds} 秒内就绪：{scheduler_address}")


def wait_for_workers(scheduler_address: str, expected_workers: int, timeout_seconds: int = 20) -> None:
    """等待指定数量的 Worker 注册到 Scheduler。"""

    deadline = time.monotonic() + timeout_seconds
    channel = grpc.insecure_channel(scheduler_address)
    stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
    try:
        while time.monotonic() < deadline:
            response = stub.ListWorkers(task_scheduler_pb2.Empty(), timeout=2)
            alive_count = sum(1 for worker in response.workers if worker.alive)
            if alive_count >= expected_workers:
                return
            time.sleep(0.5)
    finally:
        channel.close()
    raise RuntimeError(f"Worker 未在 {timeout_seconds} 秒内全部注册，期望 {expected_workers} 个。")


def ensure_scheduler_not_running(scheduler_address: str) -> None:
    """避免误把任务提交到旧 Scheduler。"""

    try:
        wait_for_scheduler(scheduler_address, timeout_seconds=2)
    except RuntimeError:
        return
    raise RuntimeError(
        f"{scheduler_address} 已经存在可用 Scheduler。"
        "请先停止旧进程，或使用 --use-existing-scheduler。"
    )


def stop_processes(processes: Iterable[subprocess.Popen[bytes]]) -> None:
    """终止本脚本启动的子进程。"""

    alive_processes = [process for process in processes if process.poll() is None]
    for process in reversed(alive_processes):
        process.terminate()

    deadline = time.monotonic() + 5
    for process in reversed(alive_processes):
        remaining = max(deadline - time.monotonic(), 0.1)
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    raise SystemExit(main())
