"""调度器服务启动入口。

运行方式：

    python -m distributed_scheduler.scheduler.server

启动后，调度器会监听 ``config/scheduler.yaml`` 中配置的地址和端口。
"""

from __future__ import annotations

import argparse
import logging
import time
from concurrent import futures

import grpc

from distributed_scheduler.common.config_loader import load_scheduler_config
from distributed_scheduler.common.logging_utils import configure_logging
from distributed_scheduler.generated import task_scheduler_pb2_grpc
from distributed_scheduler.scheduler.service import SchedulerService
from distributed_scheduler.scheduler.task_store import TaskStore


def build_arg_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="启动 gRPC 分布式任务调度器。")
    parser.add_argument("--host", help="覆盖配置文件中的监听主机。")
    parser.add_argument("--port", type=int, help="覆盖配置文件中的监听端口。")
    parser.add_argument(
        "--strategy",
        choices=["round_robin", "least_loaded", "weighted_score"],
        help="覆盖配置文件中的负载均衡策略。",
    )
    return parser


def serve() -> None:
    """启动调度器 gRPC 服务并阻塞运行。"""

    config = load_scheduler_config()
    args = build_arg_parser().parse_args()

    host = args.host or config["host"]
    port = args.port or int(config["port"])
    strategy = args.strategy or config["strategy"]

    configure_logging("scheduler")

    store = TaskStore(
        strategy=strategy,
        worker_ttl_seconds=int(config["worker_ttl_seconds"]),
        task_timeout_seconds=int(config["task_timeout_seconds"]),
    )

    # gRPC 服务端使用线程池处理 RPC 请求。max_workers 越大，可同时处理的请求越多。
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=int(config["max_workers"])))
    task_scheduler_pb2_grpc.add_SchedulerServiceServicer_to_server(SchedulerService(store), server)

    listen_address = f"{host}:{port}"
    server.add_insecure_port(listen_address)
    server.start()

    logging.info("调度器已启动 address=%s strategy=%s", listen_address, strategy)
    logging.info("按 Ctrl+C 停止调度器。")

    try:
        while True:
            # 周期性回收超时任务。即使没有 Worker 拉取任务，也能及时更新任务状态。
            requeued_count = store.requeue_timed_out_tasks()
            if requeued_count:
                logging.warning("本轮回收超时任务数量=%s", requeued_count)
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("收到停止信号，正在关闭调度器。")
        server.stop(grace=3)


if __name__ == "__main__":
    serve()
