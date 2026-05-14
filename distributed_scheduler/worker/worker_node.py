"""Worker 节点启动入口。

运行方式：

    python -m distributed_scheduler.worker.worker_node --worker-id worker-1

可以启动多个 Worker，使用不同的 ``--worker-id`` 和 ``--listen-port``。
"""

from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
from concurrent import futures

import grpc
import psutil

from distributed_scheduler.common.config_loader import load_worker_config
from distributed_scheduler.common.logging_utils import configure_logging
from distributed_scheduler.common.time_utils import now_unix_ms
from distributed_scheduler.generated import task_scheduler_pb2, task_scheduler_pb2_grpc
from distributed_scheduler.worker.executor import execute_task
from distributed_scheduler.worker.service import WorkerService


class WorkerNode:
    """Worker 节点主类。

    一个 WorkerNode 同时做三件事：

    1. 启动本地 WorkerService，提供健康检查。
    2. 定期向调度器发送心跳。
    3. 持续从调度器拉取任务，并交给本地线程池执行。
    """

    def __init__(
        self,
        worker_id: str,             # 当前 Worker 的唯一标识符
        scheduler_address: str,     # 调度器服务的地址（如 "127.0.0.1:50051"）
        listen_host: str,           # 本机监听健康检查请求的 Host
        listen_port: int,           # 本机监听健康检查请求的 Port
        heartbeat_interval_seconds: int,    # 向调度器发送心跳的频率（秒）
        max_concurrent_tasks: int,          # 当前节点允许同时执行的最大任务数（并发度）
        poll_interval_seconds: int,         # 向调度器拉取任务的轮询间隔（秒）
        max_sleep_seconds: int,             # 当拉取不到任务或发生异常时的最大退避休眠时间（秒）
    ) -> None:
        """初始化 Worker 节点配置和运行状态。"""

        self.worker_id = worker_id
        self.scheduler_address = scheduler_address
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.max_concurrent_tasks = max_concurrent_tasks
        self.poll_interval_seconds = poll_interval_seconds
        self.max_sleep_seconds = max_sleep_seconds

        # running_tasks 由多个线程读写，因此用锁保护。
        self._lock = threading.Lock()
        self._running_tasks = 0
        self._stopped = threading.Event()

        # 连接调度器的 gRPC channel 和 stub。所有心跳、拉取、上报结果都通过它完成。
        self._scheduler_channel = grpc.insecure_channel(scheduler_address)
        self._scheduler_stub = task_scheduler_pb2_grpc.SchedulerServiceStub(self._scheduler_channel)

        # 执行任务的线程池。max_workers 对应 Worker 自己声明的并发能力。
        self._executor = futures.ThreadPoolExecutor(max_workers=max_concurrent_tasks)

    def start(self) -> None:
        """启动 Worker，并阻塞直到收到 Ctrl+C。"""

        grpc_server = self._start_worker_service()

        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        poll_thread = threading.Thread(target=self._poll_loop, name="poll", daemon=True)
        heartbeat_thread.start()
        poll_thread.start()

        logging.info(
            "Worker 已启动 worker_id=%s scheduler=%s listen=%s:%s concurrency=%s",
            self.worker_id,
            self.scheduler_address,
            self.listen_host,
            self.listen_port,
            self.max_concurrent_tasks,
        )
        logging.info("按 Ctrl+C 停止 Worker。")

        try:
            while not self._stopped.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            logging.info("收到停止信号，正在关闭 Worker。")
        finally:
            self._stopped.set()
            grpc_server.stop(grace=3)
            self._executor.shutdown(wait=True)
            self._scheduler_channel.close()

    def _start_worker_service(self) -> grpc.Server:
        """启动 Worker 本地健康检查 gRPC 服务。"""

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        task_scheduler_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(self.worker_id), server)
        server.add_insecure_port(f"{self.listen_host}:{self.listen_port}")
        server.start()
        return server

    def _heartbeat_loop(self) -> None:
        """心跳循环：定期向调度器报告 Worker 当前状态。"""

        while not self._stopped.is_set():
            try:
                request = task_scheduler_pb2.WorkerHeartbeatRequest(
                    worker_id=self.worker_id,
                    address=self._public_address(),
                    max_concurrent_tasks=self.max_concurrent_tasks,
                    running_tasks=self._get_running_tasks(),
                    cpu_percent=psutil.cpu_percent(interval=None),
                    memory_percent=psutil.virtual_memory().percent,
                    timestamp_unix_ms=now_unix_ms(),
                )
                self._scheduler_stub.WorkerHeartbeat(request, timeout=5)
            except grpc.RpcError as exc:
                logging.warning("发送心跳失败：%s", exc.details() or exc.code())
            except Exception as exc:  # noqa: BLE001 - 心跳线程不能因为一次异常退出。
                logging.warning("发送心跳时出现异常：%s", exc)

            self._stopped.wait(self.heartbeat_interval_seconds)

    def _poll_loop(self) -> None:
        """任务拉取循环：有本地容量时向调度器请求任务。"""

        while not self._stopped.is_set():
            if self._get_running_tasks() >= self.max_concurrent_tasks:
                self._stopped.wait(self.poll_interval_seconds)
                continue

            try:
                response = self._scheduler_stub.PullTask(
                    task_scheduler_pb2.PullTaskRequest(worker_id=self.worker_id),
                    timeout=5,
                )
                if response.has_task:
                    self._start_task(response.task)
            except grpc.RpcError as exc:
                logging.warning("拉取任务失败：%s", exc.details() or exc.code())
            except Exception as exc:  # noqa: BLE001 - 拉取线程不能因为一次异常退出。
                logging.warning("拉取任务时出现异常：%s", exc)

            self._stopped.wait(self.poll_interval_seconds)

    def _start_task(self, task: task_scheduler_pb2.TaskInfo) -> None:
        """把调度器分配的任务提交到本地线程池。"""

        self._increment_running_tasks()
        logging.info("开始执行任务 task_id=%s name=%s", task.task_id, task.name)

        future = self._executor.submit(execute_task, task, self.max_sleep_seconds)
        future.add_done_callback(lambda done_future: self._report_task_result(task, done_future))

    def _report_task_result(
        self,
        task: task_scheduler_pb2.TaskInfo,
        done_future: futures.Future,
    ) -> None:
        """任务执行完成后，把结果回传给调度器。"""

        try:
            execution_result = done_future.result()
            request = task_scheduler_pb2.ReportTaskResultRequest(
                worker_id=self.worker_id,
                task_id=task.task_id,
                success=execution_result.success,
                result=execution_result.result,
                error=execution_result.error,
                finished_at_unix_ms=now_unix_ms(),
            )
            response = self._scheduler_stub.ReportTaskResult(request, timeout=5)
            if response.accepted:
                logging.info("任务结果已回传 task_id=%s", task.task_id)
            else:
                logging.warning("调度器拒绝任务结果 task_id=%s reason=%s", task.task_id, response.message)
        except grpc.RpcError as exc:
            logging.warning("回传任务结果失败 task_id=%s error=%s", task.task_id, exc.details() or exc.code())
        except Exception as exc:  # noqa: BLE001 - 回调中需要保护线程池线程。
            logging.warning("回传任务结果时出现异常 task_id=%s error=%s", task.task_id, exc)
        finally:
            self._decrement_running_tasks()

    def _get_running_tasks(self) -> int:
        """读取当前正在执行的任务数量。"""

        with self._lock:
            return self._running_tasks

    def _increment_running_tasks(self) -> None:
        """正在执行任务数量加一。"""

        with self._lock:
            self._running_tasks += 1

    def _decrement_running_tasks(self) -> None:
        """正在执行任务数量减一。"""

        with self._lock:
            self._running_tasks = max(self._running_tasks - 1, 0)

    def _public_address(self) -> str:
        """返回 Worker 对外展示的地址。

        如果配置中使用 0.0.0.0 监听，心跳里展示本机 hostname，便于区分节点。
        """

        host = socket.gethostname() if self.listen_host == "0.0.0.0" else self.listen_host
        return f"{host}:{self.listen_port}"


def build_arg_parser() -> argparse.ArgumentParser:
    """创建 Worker 命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="启动 gRPC 分布式任务 Worker。")
    parser.add_argument("--worker-id", required=True, help="Worker 唯一标识，例如 worker-1。")
    parser.add_argument("--scheduler-address", help="调度器地址，例如 127.0.0.1:50051。")
    parser.add_argument("--listen-host", help="Worker 本地 gRPC 监听主机。")
    parser.add_argument("--listen-port", type=int, help="Worker 本地 gRPC 监听端口。")
    parser.add_argument("--max-concurrent-tasks", type=int, help="Worker 最大并发任务数。")
    return parser


def main() -> None:
    """Worker 命令行入口。"""

    config = load_worker_config()
    args = build_arg_parser().parse_args()

    configure_logging(args.worker_id)

    worker = WorkerNode(
        worker_id=args.worker_id,
        scheduler_address=args.scheduler_address or config["scheduler_address"],
        listen_host=args.listen_host or config["listen_host"],
        listen_port=args.listen_port or int(config["listen_port"]),
        heartbeat_interval_seconds=int(config["heartbeat_interval_seconds"]),
        max_concurrent_tasks=args.max_concurrent_tasks or int(config["max_concurrent_tasks"]),
        poll_interval_seconds=int(config["poll_interval_seconds"]),
        max_sleep_seconds=int(config["max_sleep_seconds"]),
    )
    worker.start()


if __name__ == "__main__":
    main()
