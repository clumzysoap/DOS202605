"""Worker 任务执行器。

为了课程演示安全，Worker 不执行用户上传的任意 Python 源码，而是提供几个固定任务：

- sleep：模拟 I/O 等待型任务。
- fibonacci：模拟 CPU 计算型任务。
- word_count：模拟文本处理任务。

这样既能展示任务调度，又不会引入执行任意代码的安全问题。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from distributed_scheduler.generated import task_scheduler_pb2


@dataclass
class ExecutionResult:
    """任务执行结果。

    Attributes:
        success: 是否执行成功。
        result: 成功时的结果文本。
        error: 失败时的错误信息。
    """

    success: bool
    result: str = ""
    error: str = ""


def execute_task(task: task_scheduler_pb2.TaskInfo, max_sleep_seconds: int) -> ExecutionResult:
    """根据任务类型执行具体逻辑。

    Args:
        task: 调度器分配的任务信息。
        max_sleep_seconds: sleep 任务允许的最大秒数。

    Returns:
        ExecutionResult，调用方会把它回传给调度器。
    """

    try:
        if task.task_type == task_scheduler_pb2.TASK_TYPE_SLEEP:
            return _execute_sleep(task.payload, max_sleep_seconds)

        if task.task_type == task_scheduler_pb2.TASK_TYPE_FIBONACCI:
            return _execute_fibonacci(task.payload)

        if task.task_type == task_scheduler_pb2.TASK_TYPE_WORD_COUNT:
            return _execute_word_count(task.payload)

        return ExecutionResult(success=False, error=f"未知任务类型：{task.task_type}")
    except Exception as exc:  # noqa: BLE001 - 课程项目需要把异常转成任务失败结果。
        return ExecutionResult(success=False, error=f"{type(exc).__name__}: {exc}")


def _execute_sleep(payload: str, max_sleep_seconds: int) -> ExecutionResult:
    """执行 sleep 任务。

    payload 应该是秒数，例如 ``3``。该任务用于模拟网络请求、磁盘等待等 I/O 型任务。
    """

    seconds = float(payload)
    if seconds < 0:
        raise ValueError("sleep 秒数不能为负数。")
    if seconds > max_sleep_seconds:
        raise ValueError(f"sleep 秒数不能超过 {max_sleep_seconds}。")

    time.sleep(seconds)
    return ExecutionResult(success=True, result=f"已休眠 {seconds:.2f} 秒。")


def _execute_fibonacci(payload: str) -> ExecutionResult:
    """执行 Fibonacci 任务。

    payload 应该是非负整数 n。这里使用迭代算法，避免递归版本在较大 n 时过慢。
    """

    n = int(payload)
    if n < 0:
        raise ValueError("Fibonacci 参数 n 不能为负数。")
    if n > 100000:
        raise ValueError("Fibonacci 参数 n 过大，课程演示限制为 100000 以内。")

    previous, current = 0, 1
    for _ in range(n):
        previous, current = current, previous + current

    return ExecutionResult(success=True, result=f"fib({n}) = {previous}")

def cal_fibonacci_work(number : int) -> int :
    if number == 2 or number == 1 : return number
    else : return cal_fibonacci_work(number - 1) + cal_fibonacci_work(number - 2)

def _execute_word_count(payload: str) -> ExecutionResult:
    """执行单词统计任务。

    对英文文本按空白分词；对中文文本也可以统计“非空白片段”数量。这个任务用于展示
    Worker 可以处理带字符串参数的任务。
    """

    words = [word for word in payload.split() if word.strip()]
    characters = len(payload)
    return ExecutionResult(
        success=True,
        result=f"word_count={len(words)}, char_count={characters}",
    )
