"""YAML 配置加载器。

课程项目需要多个可执行入口。如果每个入口都手写配置读取逻辑，后续维护会变得混乱。
本模块提供一个轻量封装：读取 YAML 文件，如果文件不存在或缺少字段，就使用默认值。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


# 项目根目录。
# 当前文件路径为 distributed_scheduler/common/config_loader.py，
# parents[2] 正好回到仓库/项目根目录。
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_yaml_config(relative_path: str, defaults: dict[str, Any]) -> dict[str, Any]:
    """读取 YAML 配置并合并默认值。

    Args:
        relative_path: 相对于项目根目录的配置文件路径，例如 ``config/scheduler.yaml``。
        defaults: 默认配置。YAML 文件中缺失的字段会从这里补齐。

    Returns:
        合并后的配置字典。

    Raises:
        ValueError: YAML 文件内容不是字典时抛出，避免后续代码拿到奇怪结构。
    """

    path = PROJECT_ROOT / relative_path

    # 先复制默认值，避免调用者传入的 defaults 被意外修改。
    config = dict(defaults)

    if not path.exists():
        return config

    with path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file) or {}

    if not isinstance(loaded, dict):
        raise ValueError(f"配置文件 {path} 的顶层结构必须是 YAML 字典。")

    config.update(loaded)
    return config


def load_scheduler_config() -> dict[str, Any]:
    """读取调度器配置。"""

    return load_yaml_config(
        "config/scheduler.yaml",
        {
            "host": "0.0.0.0",
            "port": 50051,
            "strategy": "weighted_score",
            "worker_ttl_seconds": 15,
            "task_timeout_seconds": 60,
            "max_workers": 16,
        },
    )


def load_worker_config() -> dict[str, Any]:
    """读取 Worker 配置。"""

    return load_yaml_config(
        "config/worker.yaml",
        {
            "scheduler_address": "127.0.0.1:50051",
            "listen_host": "0.0.0.0",
            "listen_port": 50061,
            "heartbeat_interval_seconds": 3,
            "max_concurrent_tasks": 2,
            "poll_interval_seconds": 1,
            "max_sleep_seconds": 10,
        },
    )
