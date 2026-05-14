"""时间工具函数。

分布式系统中不同进程会频繁交换时间信息，例如任务创建时间、任务开始时间、
Worker 心跳时间等。本项目统一使用“Unix 毫秒时间戳”，原因是：

1. 与 gRPC / Protobuf 的整数类型兼容，不需要额外处理时区。
2. 人类调试时也可以很容易地转换为真实时间。
3. 毫秒精度足够支撑课程演示中的任务调度和超时判断。
"""

from __future__ import annotations

import time


def now_unix_ms() -> int:
    """返回当前 Unix 毫秒时间戳。

    Python 的 ``time.time()`` 返回秒级浮点数；这里乘以 1000 并转成整数，
    让所有模块都使用统一格式。
    """

    return int(time.time() * 1000)


def seconds_since(timestamp_unix_ms: int) -> float:
    """计算某个毫秒时间戳距离现在已经过去多少秒。

    Args:
        timestamp_unix_ms: 历史时间点，单位是毫秒。

    Returns:
        当前时间与历史时间的差值，单位是秒。
    """

    if timestamp_unix_ms <= 0:
        return float("inf")
    return (now_unix_ms() - timestamp_unix_ms) / 1000.0
