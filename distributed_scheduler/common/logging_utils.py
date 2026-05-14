"""日志初始化工具。

系统运行时通常会同时启动多个进程：一个调度器、多个 Worker、一个或多个客户端。
统一日志格式可以帮助观察“任务提交 -> Worker 拉取 -> 执行完成 -> 回传结果”的完整链路。
"""

from __future__ import annotations

import logging
import sys


def configure_logging(service_name: str, level: int = logging.INFO) -> None:
    """初始化根日志配置。

    Args:
        service_name: 当前进程名称，例如 ``scheduler``、``worker-1`` 或 ``client``。
        level: 日志级别，默认使用 INFO，既能看到关键流程，又不会太吵。

    Notes:
        ``logging.basicConfig`` 只在第一次调用时生效。为了让重复运行测试或交互式
        调试时也能刷新格式，这里传入 ``force=True``。
    """

    logging.basicConfig(
        level=level,
        format=f"%(asctime)s | {service_name} | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
