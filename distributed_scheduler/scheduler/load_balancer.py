"""负载均衡策略实现。

本项目把“如何选择 Worker”从调度器主逻辑中拆出来，便于课堂展示不同策略的效果。
调度器只负责提供可用 Worker 列表，具体选择逻辑由本模块完成。
"""

from __future__ import annotations

from dataclasses import dataclass

from distributed_scheduler.common.models import WorkerRecord


@dataclass
class LoadBalancer:
    """根据指定策略从候选 Worker 中选择一个节点。

    Attributes:
        strategy: 策略名，可选 ``round_robin``、``least_loaded``、``weighted_score``。
        _round_robin_index: 轮询策略的内部游标。
    """

    strategy: str
    _round_robin_index: int = 0

    def choose(self, workers: list[WorkerRecord]) -> WorkerRecord | None:
        """从候选 Worker 中选择一个。

        Args:
            workers: 已经过滤为“存活且有容量”的 Worker 列表。

        Returns:
            被选中的 Worker；如果没有候选节点，则返回 ``None``。
        """

        if not workers:
            return None

        # 按 worker_id 排序可以让相同状态下的选择结果稳定，方便课堂演示和测试。
        ordered_workers = sorted(workers, key=lambda worker: worker.worker_id)

        if self.strategy == "round_robin":
            return self._choose_round_robin(ordered_workers)

        if self.strategy == "least_loaded":
            return self._choose_least_loaded(ordered_workers)

        if self.strategy == "weighted_score":
            return self._choose_weighted_score(ordered_workers)

        # 如果配置文件写错策略名，不让系统崩溃，而是退回综合加权策略。
        return self._choose_weighted_score(ordered_workers)

    def _choose_round_robin(self, workers: list[WorkerRecord]) -> WorkerRecord:
        """轮询策略：每次选择下一个 Worker。

        轮询不考虑 CPU 和内存，只追求请求数量大致均匀。它适合所有节点性能接近的场景。
        """

        index = self._round_robin_index % len(workers)
        self._round_robin_index += 1
        return workers[index]

    @staticmethod
    def _choose_least_loaded(workers: list[WorkerRecord]) -> WorkerRecord:
        """最少任务数策略：优先选择正在运行任务数量最少的 Worker。

        如果两个 Worker 当前任务数相同，再比较 CPU 和内存，让选择结果更合理。
        """

        return min(
            workers,
            key=lambda worker: (
                worker.running_tasks,
                worker.cpu_percent,
                worker.memory_percent,
                worker.worker_id,
            ),
        )

    @staticmethod
    def _choose_weighted_score(workers: list[WorkerRecord]) -> WorkerRecord:
        """综合加权策略：优先选择综合负载分数最低的 Worker。"""

        return min(workers, key=lambda worker: (worker.load_score(), worker.worker_id))
