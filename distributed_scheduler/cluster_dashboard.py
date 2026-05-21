"""Local web dashboard for the demo cluster launcher.

The dashboard intentionally uses only the Python standard library plus the
project's existing gRPC dependencies. It wraps the same scheduler/worker
processes used by ``scripts/start_demo_cluster.py`` and exposes a small JSON API
for a browser UI.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
import webbrowser
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import grpc

from distributed_scheduler.common.logging_utils import configure_logging
from distributed_scheduler.common.models import task_status_to_text, task_type_from_cli, task_type_to_text
from distributed_scheduler.common.time_utils import now_unix_ms
from distributed_scheduler.generated import task_scheduler_pb2, task_scheduler_pb2_grpc


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEDULER_HOST = "127.0.0.1"
DEFAULT_SCHEDULER_PORT = 50051
DEFAULT_WORKER_START_PORT = 50061
DEFAULT_WORKER_COUNT = 5
DEFAULT_WORKER_CONCURRENCY = 2
DEFAULT_TASK_COUNT = 200
DEFAULT_TASK_LIST_LIMIT = 200
DEFAULT_DASHBOARD_HOST = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 8765
ALLOWED_STRATEGIES = ("round_robin", "least_loaded", "weighted_score")
TASK_TYPES = ("sleep", "fibonacci", "word_count")


class ClusterCancelled(RuntimeError):
    """Raised when a launch is cancelled by a stop request."""


@dataclass(slots=True)
class TaskTemplate:
    """Task batch row configured by the dashboard."""

    task_type: str
    count: int
    payload: str
    priority: int = 0
    name_prefix: str = ""

    def normalized_type(self) -> str:
        return self.task_type.strip().lower().replace("-", "_")

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_type": self.normalized_type(),
            "count": self.count,
            "payload": self.payload,
            "priority": self.priority,
            "name_prefix": self.name_prefix,
        }

    def build_request(self, index: int) -> task_scheduler_pb2.SubmitTaskRequest:
        task_type = self.normalized_type()
        prefix = self.name_prefix.strip() or f"demo-{task_type}"
        return task_scheduler_pb2.SubmitTaskRequest(
            name=f"{prefix}-{index:03d}",
            task_type=task_type_from_cli(task_type),
            payload=str(self.payload),
            priority=int(self.priority),
        )


@dataclass(slots=True)
class WorkerSpec:
    """Worker 启动时使用的独立配置。"""

    worker_id: str
    listen_port: int
    max_concurrent_tasks: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "listen_port": self.listen_port,
            "max_concurrent_tasks": self.max_concurrent_tasks,
        }


@dataclass(slots=True)
class ClusterConfig:
    """Configuration for one visualized demo cluster run."""

    scheduler_host: str = DEFAULT_SCHEDULER_HOST
    scheduler_port: int = DEFAULT_SCHEDULER_PORT
    strategy: str = "weighted_score"
    worker_count: int = DEFAULT_WORKER_COUNT
    worker_start_port: int = DEFAULT_WORKER_START_PORT
    worker_concurrency: int = DEFAULT_WORKER_CONCURRENCY
    worker_specs: list[WorkerSpec] = field(default_factory=list)
    task_templates: list[TaskTemplate] = field(default_factory=lambda: default_task_templates(DEFAULT_TASK_COUNT))
    task_limit: int = DEFAULT_TASK_LIST_LIMIT
    start_workers_first: bool = False
    use_existing_scheduler: bool = False
    worker_listen_host: str = "127.0.0.1"

    @property
    def scheduler_address(self) -> str:
        return f"{self.scheduler_host}:{self.scheduler_port}"

    @property
    def total_task_count(self) -> int:
        return sum(template.count for template in self.task_templates)

    def resolved_worker_specs(self) -> list[WorkerSpec]:
        if self.worker_specs:
            return list(self.worker_specs)
        return default_worker_specs(self.worker_count, self.worker_start_port, self.worker_concurrency)

    def to_dict(self) -> dict[str, Any]:
        worker_specs = self.resolved_worker_specs()
        default_worker_concurrency = worker_specs[0].max_concurrent_tasks if worker_specs else self.worker_concurrency
        return {
            "scheduler_host": self.scheduler_host,
            "scheduler_port": self.scheduler_port,
            "scheduler_address": self.scheduler_address,
            "strategy": self.strategy,
            "worker_count": len(worker_specs),
            "worker_start_port": worker_specs[0].listen_port if worker_specs else self.worker_start_port,
            "worker_concurrency": default_worker_concurrency,
            "worker_specs": [worker_spec.to_dict() for worker_spec in worker_specs],
            "task_limit": self.task_limit,
            "start_workers_first": self.start_workers_first,
            "use_existing_scheduler": self.use_existing_scheduler,
            "worker_listen_host": self.worker_listen_host,
            "total_task_count": self.total_task_count,
            "task_templates": [template.to_dict() for template in self.task_templates],
        }


@dataclass(slots=True)
class ManagedProcess:
    name: str
    kind: str
    process: subprocess.Popen[bytes]
    log_path: Path

    def to_dict(self) -> dict[str, Any]:
        exit_code = self.process.poll()
        return {
            "name": self.name,
            "kind": self.kind,
            "pid": self.process.pid,
            "alive": exit_code is None,
            "exit_code": exit_code,
            "log_path": str(self.log_path),
        }


@dataclass(slots=True)
class RunContext:
    config: ClusterConfig
    cancel_event: threading.Event
    started_at_unix_ms: int
    processes: list[ManagedProcess] = field(default_factory=list)


class DemoClusterController:
    """Owns demo cluster subprocesses and reads live gRPC state."""

    def __init__(self, project_root: Path = PROJECT_ROOT) -> None:
        self._project_root = project_root
        self._lock = threading.RLock()
        self._phase = "idle"
        self._message = "Dashboard ready. Configure a cluster and press Start."
        self._last_error = ""
        self._config: ClusterConfig | None = None
        self._run_context: RunContext | None = None
        self._processes: list[ManagedProcess] = []
        self._submitted_tasks = 0
        self._task_stats_by_worker: dict[str, dict[str, Any]] = {}

    def start_async(self, config: ClusterConfig) -> tuple[bool, str]:
        """Start a cluster in a background thread."""

        with self._lock:
            if self._phase in {"starting", "running", "stopping"}:
                return False, "A cluster is already starting or running. Stop it before starting another one."

            context = RunContext(
                config=config,
                cancel_event=threading.Event(),
                started_at_unix_ms=now_unix_ms(),
            )
            self._phase = "starting"
            self._message = "Starting scheduler, workers, and task batch..."
            self._last_error = ""
            self._config = config
            self._run_context = context
            self._processes = []
            self._submitted_tasks = 0
            self._task_stats_by_worker = {}

        thread = threading.Thread(target=self._launch_cluster, args=(context,), name="demo-cluster-launch", daemon=True)
        thread.start()
        return True, self._message

    def start_blocking(self, config: ClusterConfig, timeout_seconds: int = 60) -> None:
        accepted, message = self.start_async(config)
        if not accepted:
            raise RuntimeError(message)

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            with self._lock:
                phase = self._phase
                current_message = self._message
            if phase == "running":
                return
            if phase == "error":
                raise RuntimeError(current_message)
            time.sleep(0.25)
        raise TimeoutError("Cluster did not finish startup in time.")

    def stop(self) -> None:
        """Stop subprocesses started by this dashboard."""

        with self._lock:
            context = self._run_context
            processes = list(self._processes)
            if context is not None:
                context.cancel_event.set()
            if processes:
                self._phase = "stopping"
                self._message = "Stopping demo cluster..."
            else:
                self._phase = "idle"
                self._message = "Cluster is stopped."

        self._stop_processes(processes)

        with self._lock:
            self._processes = []
            self._run_context = None
            self._phase = "idle"
            self._message = "Cluster is stopped."

    def exited_processes(self) -> list[dict[str, Any]]:
        with self._lock:
            processes = list(self._processes)
        return [process.to_dict() for process in processes if process.process.poll() is not None]

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-ready snapshot for the browser UI."""

        with self._lock:
            phase = self._phase
            message = self._message
            last_error = self._last_error
            config = self._config
            submitted_tasks = self._submitted_tasks
            started_at = self._run_context.started_at_unix_ms if self._run_context else 0
            processes = list(self._processes)

        scheduler_address = config.scheduler_address if config else f"{DEFAULT_SCHEDULER_HOST}:{DEFAULT_SCHEDULER_PORT}"
        scheduler = {
            "address": scheduler_address,
            "healthy": False,
            "message": "No scheduler configured yet.",
        }
        workers: list[dict[str, Any]] = []
        all_tasks: list[dict[str, Any]] = []

        if phase != "idle":
            scheduler, workers, all_tasks = self._read_scheduler_state(scheduler_address)

        task_limit = config.task_limit if config else DEFAULT_TASK_LIST_LIMIT
        task_counts = Counter(task["status"] for task in all_tasks)
        alive_workers = sum(1 for worker in workers if worker["alive"])
        process_dicts = [process.to_dict() for process in processes]
        process_exits = [process for process in process_dicts if not process["alive"]]
        worker_stats = self._build_worker_task_stats(workers, all_tasks, config)
        with self._lock:
            self._task_stats_by_worker = {item["worker_id"]: item for item in worker_stats}

        summary = {
            "expected_tasks": config.total_task_count if config else 0,
            "submitted_tasks": submitted_tasks,
            "visible_task_limit": task_limit,
            "total_tasks": len(all_tasks),
            "queued": task_counts.get("queued", 0),
            "running": task_counts.get("running", 0),
            "succeeded": task_counts.get("succeeded", 0),
            "failed": task_counts.get("failed", 0),
            "timed_out": task_counts.get("timed_out", 0),
            "completed": task_counts.get("succeeded", 0) + task_counts.get("failed", 0) + task_counts.get("timed_out", 0),
            "configured_workers": config.worker_count if config else 0,
            "alive_workers": alive_workers,
            "known_workers": len(workers),
            "process_exits": process_exits,
        }

        return {
            "phase": phase,
            "message": message,
            "last_error": last_error,
            "started_at_unix_ms": started_at,
            "server_time_unix_ms": now_unix_ms(),
            "config": config.to_dict() if config else None,
            "scheduler": scheduler,
            "summary": summary,
            "workers": workers,
            "worker_stats": worker_stats,
            "tasks": all_tasks[:task_limit],
            "processes": process_dicts,
        }

    def _launch_cluster(self, context: RunContext) -> None:
        config = context.config
        log_dir = self._project_root / "logs" / "demo_cluster"
        log_dir.mkdir(parents=True, exist_ok=True)

        try:
            if config.use_existing_scheduler:
                self._set_message(f"Connecting to existing scheduler at {config.scheduler_address}...")
                self._wait_for_scheduler(config.scheduler_address, timeout_seconds=15, cancel_event=context.cancel_event)
            else:
                self._set_message(f"Checking scheduler port {config.scheduler_address}...")
                self._ensure_scheduler_not_running(config.scheduler_address, context.cancel_event)
                scheduler = self._start_scheduler(config, log_dir)
                context.processes.append(scheduler)
                self._wait_for_scheduler(config.scheduler_address, timeout_seconds=15, cancel_event=context.cancel_event)
                self._set_message(f"Scheduler started at {config.scheduler_address}.")

            if config.start_workers_first:
                self._set_message("Starting workers before submitting tasks...")
                context.processes.extend(self._start_workers(config, log_dir))
                self._wait_for_workers(config.scheduler_address, config.worker_count, timeout_seconds=20, cancel_event=context.cancel_event)
                self._submit_tasks(config, context.cancel_event)
            else:
                self._set_message("Submitting tasks before starting workers...")
                self._submit_tasks(config, context.cancel_event)
                self._set_message("Starting workers after task submission...")
                context.processes.extend(self._start_workers(config, log_dir))
                self._wait_for_workers(config.scheduler_address, config.worker_count, timeout_seconds=20, cancel_event=context.cancel_event)

            context.cancel_event.wait(0.1)
            if context.cancel_event.is_set():
                raise ClusterCancelled("Cluster startup was cancelled.")

            with self._lock:
                if self._run_context is context:
                    self._phase = "running"
                    self._message = "Cluster is running. Live state is refreshed from SchedulerService."

        except ClusterCancelled as exc:
            self._stop_processes(context.processes)
            with self._lock:
                if self._run_context is context:
                    self._phase = "idle"
                    self._message = str(exc)
                    self._run_context = None
                    self._processes = []
        except Exception as exc:  # noqa: BLE001 - surface launch failures in the dashboard.
            logging.exception("Failed to start demo cluster.")
            self._stop_processes(context.processes)
            with self._lock:
                if self._run_context is context:
                    self._phase = "error"
                    self._message = f"{type(exc).__name__}: {exc}"
                    self._last_error = self._message
                    self._processes = []

    def _set_message(self, message: str) -> None:
        with self._lock:
            self._message = message

    def _add_process(self, managed_process: ManagedProcess) -> None:
        with self._lock:
            self._processes.append(managed_process)

    def _start_scheduler(self, config: ClusterConfig, log_dir: Path) -> ManagedProcess:
        log_path = log_dir / "scheduler.log"
        command = [
            sys.executable,
            "-m",
            "distributed_scheduler.scheduler.server",
            "--host",
            config.scheduler_host,
            "--port",
            str(config.scheduler_port),
            "--strategy",
            config.strategy,
        ]
        with log_path.open("ab") as log_file:
            process = subprocess.Popen(command, cwd=self._project_root, stdout=log_file, stderr=subprocess.STDOUT)
        managed = ManagedProcess(name="scheduler", kind="scheduler", process=process, log_path=log_path)
        self._add_process(managed)
        return managed

    def _start_workers(self, config: ClusterConfig, log_dir: Path) -> list[ManagedProcess]:
        workers: list[ManagedProcess] = []
        worker_specs = config.resolved_worker_specs()
        for worker_spec in worker_specs:
            worker_id = worker_spec.worker_id
            listen_port = worker_spec.listen_port
            log_path = log_dir / f"{worker_id}.log"
            command = [
                sys.executable,
                "-m",
                "distributed_scheduler.worker.worker_node",
                "--worker-id",
                worker_id,
                "--scheduler-address",
                config.scheduler_address,
                "--listen-host",
                config.worker_listen_host,
                "--listen-port",
                str(listen_port),
                "--max-concurrent-tasks",
                str(worker_spec.max_concurrent_tasks),
            ]
            with log_path.open("ab") as log_file:
                process = subprocess.Popen(command, cwd=self._project_root, stdout=log_file, stderr=subprocess.STDOUT)
            managed = ManagedProcess(name=worker_id, kind="worker", process=process, log_path=log_path)
            self._add_process(managed)
            workers.append(managed)
            self._set_message(f"Worker {worker_id} started on port {listen_port} with concurrency {worker_spec.max_concurrent_tasks}.")
        return workers

    def _submit_tasks(self, config: ClusterConfig, cancel_event: threading.Event) -> int:
        channel = grpc.insecure_channel(config.scheduler_address)
        stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
        submitted = 0
        try:
            for request in build_task_requests(config.task_templates):
                if cancel_event.is_set():
                    raise ClusterCancelled("Cluster startup was cancelled.")
                stub.SubmitTask(request, timeout=5)
                submitted += 1
                with self._lock:
                    self._submitted_tasks = submitted
                if submitted % 25 == 0 or submitted == config.total_task_count:
                    self._set_message(f"Submitted {submitted}/{config.total_task_count} tasks.")
        finally:
            channel.close()
        return submitted

    def _read_scheduler_state(self, scheduler_address: str) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        scheduler = {
            "address": scheduler_address,
            "healthy": False,
            "message": "",
        }
        workers: list[dict[str, Any]] = []
        tasks: list[dict[str, Any]] = []
        channel = grpc.insecure_channel(scheduler_address)
        stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
        try:
            health = stub.HealthCheck(task_scheduler_pb2.Empty(), timeout=1)
            scheduler = {
                "address": scheduler_address,
                "healthy": bool(health.ok),
                "service": health.service,
                "message": health.message,
            }
            worker_response = stub.ListWorkers(task_scheduler_pb2.Empty(), timeout=2)
            workers = [worker_to_dict(worker) for worker in worker_response.workers]

            task_response = stub.ListTasks(task_scheduler_pb2.ListTasksRequest(limit=0), timeout=3)
            tasks = [task_to_dict(task) for task in task_response.tasks]
        except grpc.RpcError as exc:
            scheduler["message"] = exc.details() or str(exc.code())
        finally:
            channel.close()
        return scheduler, workers, tasks

    def _build_worker_task_stats(
        self,
        workers: list[dict[str, Any]],
        all_tasks: list[dict[str, Any]],
        config: ClusterConfig | None,
    ) -> list[dict[str, Any]]:
        worker_lookup = {worker["worker_id"]: worker for worker in workers}
        spec_lookup = {spec.worker_id: spec for spec in config.resolved_worker_specs()} if config else {}
        stats: dict[str, dict[str, Any]] = {}
        worker_order = [spec.worker_id for spec in config.resolved_worker_specs()] if config else [worker["worker_id"] for worker in workers]

        def ensure_entry(worker_id: str) -> dict[str, Any]:
            entry = stats.get(worker_id)
            if entry is not None:
                return entry

            worker = worker_lookup.get(worker_id, {})
            spec = spec_lookup.get(worker_id)
            entry = {
                "worker_id": worker_id,
                "address": worker.get("address", ""),
                "alive": bool(worker.get("alive", False)),
                "listening_port": int(worker.get("address", "").rsplit(":", 1)[-1]) if worker.get("address") and ":" in worker.get("address", "") else (spec.listen_port if spec else 0),
                "configured_max_concurrent_tasks": spec.max_concurrent_tasks if spec else int(worker.get("max_concurrent_tasks", 0) or 0),
                "current_running_tasks": int(worker.get("running_tasks", 0) or 0),
                "total_tasks": 0,
                "queued_tasks": 0,
                "running_tasks": 0,
                "succeeded_tasks": 0,
                "failed_tasks": 0,
                "timed_out_tasks": 0,
                "total_duration_ms": 0,
                "average_duration_ms": 0,
                "task_names": [],
                "task_summaries": [],
            }
            stats[worker_id] = entry
            return entry

        for worker_id in worker_order:
            ensure_entry(worker_id)

        for task in sorted(all_tasks, key=lambda item: (item["started_at_unix_ms"] or item["created_at_unix_ms"], item["task_id"]), reverse=True):
            worker_id = task.get("assigned_worker_id") or ""
            if not worker_id:
                continue
            entry = ensure_entry(worker_id)
            status = task["status"]
            duration_ms = int(task.get("duration_ms") or 0)
            entry["total_tasks"] += 1
            if status == "queued":
                entry["queued_tasks"] += 1
            elif status == "running":
                entry["running_tasks"] += 1
            elif status == "succeeded":
                entry["succeeded_tasks"] += 1
            elif status == "failed":
                entry["failed_tasks"] += 1
            elif status == "timed_out":
                entry["timed_out_tasks"] += 1
            entry["total_duration_ms"] += duration_ms
            if task["name"] not in entry["task_names"]:
                entry["task_names"].append(task["name"])
            entry["task_summaries"].append(
                {
                    "task_id": task["task_id"],
                    "short_task_id": task["short_task_id"],
                    "name": task["name"],
                    "task_type": task["task_type"],
                    "status": status,
                    "duration_ms": duration_ms,
                    "payload": task["payload"],
                    "result": task["result"],
                    "error": task["error"],
                }
            )

        for entry in stats.values():
            total_tasks = entry["total_tasks"]
            entry["average_duration_ms"] = round(entry["total_duration_ms"] / total_tasks, 2) if total_tasks else 0
            entry["task_names"] = entry["task_names"][:24]
            entry["task_summaries"] = entry["task_summaries"][:12]

        return [stats[worker_id] for worker_id in worker_order] + [entry for worker_id, entry in stats.items() if worker_id not in worker_order]

    def _wait_for_scheduler(self, scheduler_address: str, timeout_seconds: int, cancel_event: threading.Event) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if cancel_event.is_set():
                raise ClusterCancelled("Cluster startup was cancelled.")
            channel = grpc.insecure_channel(scheduler_address)
            stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
            try:
                response = stub.HealthCheck(task_scheduler_pb2.Empty(), timeout=1)
                if response.ok:
                    return
            except grpc.RpcError:
                time.sleep(0.5)
            finally:
                channel.close()
        raise RuntimeError(f"Scheduler is not ready after {timeout_seconds}s: {scheduler_address}")

    def _wait_for_workers(
        self,
        scheduler_address: str,
        expected_workers: int,
        timeout_seconds: int,
        cancel_event: threading.Event,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        channel = grpc.insecure_channel(scheduler_address)
        stub = task_scheduler_pb2_grpc.SchedulerServiceStub(channel)
        try:
            while time.monotonic() < deadline:
                if cancel_event.is_set():
                    raise ClusterCancelled("Cluster startup was cancelled.")
                response = stub.ListWorkers(task_scheduler_pb2.Empty(), timeout=2)
                alive_count = sum(1 for worker in response.workers if worker.alive)
                if alive_count >= expected_workers:
                    return
                self._set_message(f"Waiting for workers: {alive_count}/{expected_workers} alive.")
                time.sleep(0.5)
        finally:
            channel.close()
        raise RuntimeError(f"Workers did not all register after {timeout_seconds}s. Expected {expected_workers}.")

    def _ensure_scheduler_not_running(self, scheduler_address: str, cancel_event: threading.Event) -> None:
        try:
            self._wait_for_scheduler(scheduler_address, timeout_seconds=2, cancel_event=cancel_event)
        except RuntimeError:
            return
        raise RuntimeError(
            f"{scheduler_address} already has a reachable scheduler. "
            "Stop it first or enable 'use existing scheduler'."
        )

    @staticmethod
    def _stop_processes(processes: Iterable[ManagedProcess]) -> None:
        process_list = [managed.process for managed in processes if managed.process.poll() is None]
        for process in reversed(process_list):
            process.terminate()

        deadline = time.monotonic() + 5
        for process in reversed(process_list):
            remaining = max(deadline - time.monotonic(), 0.1)
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                process.kill()


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def build_task_requests(task_templates: list[TaskTemplate]) -> Iterable[task_scheduler_pb2.SubmitTaskRequest]:
    """Yield task requests by round-robin interleaving template rows."""

    templates = [template for template in task_templates if template.count > 0]
    remaining = [template.count for template in templates]
    indexes = [1 for _ in templates]
    while any(count > 0 for count in remaining):
        for template_index, template in enumerate(templates):
            if remaining[template_index] <= 0:
                continue
            yield template.build_request(indexes[template_index])
            indexes[template_index] += 1
            remaining[template_index] -= 1


def worker_to_dict(worker: task_scheduler_pb2.WorkerInfo) -> dict[str, Any]:
    capacity = max(worker.max_concurrent_tasks, 1)
    concurrency_percent = worker.running_tasks / capacity * 100.0
    load_score = worker.cpu_percent * 0.4 + worker.memory_percent * 0.3 + concurrency_percent * 0.3
    heartbeat_age_seconds = max((now_unix_ms() - worker.last_heartbeat_unix_ms) / 1000.0, 0.0)
    return {
        "worker_id": worker.worker_id,
        "address": worker.address,
        "max_concurrent_tasks": worker.max_concurrent_tasks,
        "running_tasks": worker.running_tasks,
        "cpu_percent": round(worker.cpu_percent, 2),
        "memory_percent": round(worker.memory_percent, 2),
        "concurrency_percent": round(concurrency_percent, 2),
        "load_score": round(load_score, 2),
        "last_heartbeat_unix_ms": worker.last_heartbeat_unix_ms,
        "heartbeat_age_seconds": round(heartbeat_age_seconds, 2),
        "alive": bool(worker.alive),
    }


def task_to_dict(task: task_scheduler_pb2.TaskInfo) -> dict[str, Any]:
    now_ms = now_unix_ms()
    started_at = task.started_at_unix_ms
    finished_at = task.finished_at_unix_ms
    if finished_at > 0 and started_at > 0:
        duration_ms = max(finished_at - started_at, 0)
    elif started_at > 0:
        duration_ms = max(now_ms - started_at, 0)
    else:
        duration_ms = 0

    return {
        "task_id": task.task_id,
        "short_task_id": task.task_id[:8],
        "name": task.name,
        "task_type": task_type_to_text(task.task_type),
        "payload": task.payload,
        "priority": task.priority,
        "status": task_status_to_text(task.status),
        "assigned_worker_id": task.assigned_worker_id,
        "result": task.result,
        "error": task.error,
        "created_at_unix_ms": task.created_at_unix_ms,
        "started_at_unix_ms": task.started_at_unix_ms,
        "finished_at_unix_ms": task.finished_at_unix_ms,
        "duration_ms": duration_ms,
    }


def default_worker_specs(worker_count: int, worker_start_port: int, worker_concurrency: int) -> list[WorkerSpec]:
    count = max(int(worker_count), 1)
    start_port = int(worker_start_port)
    concurrency = max(int(worker_concurrency), 1)
    return [
        WorkerSpec(
            worker_id=f"worker-{index}",
            listen_port=start_port + index - 1,
            max_concurrent_tasks=concurrency,
        )
        for index in range(1, count + 1)
    ]


def default_task_templates(total_task_count: int) -> list[TaskTemplate]:
    total = max(int(total_task_count), 1)
    base = total // 3
    remainder = total % 3
    counts = [base + (1 if index < remainder else 0) for index in range(3)]
    templates = [
        TaskTemplate("sleep", counts[0], "3", 0, "demo-sleep"),
        TaskTemplate("fibonacci", counts[1], "32", 0, "demo-fib"),
        TaskTemplate("word_count", counts[2], "distributed scheduler load balancing demo worker queue grpc", 0, "demo-word"),
    ]
    return [template for template in templates if template.count > 0]


def config_from_payload(payload: dict[str, Any]) -> ClusterConfig:
    if "worker_specs" in payload:
        worker_specs_payload = payload.get("worker_specs") or []
        worker_specs = [
            worker_spec_from_payload(item, index)
            for index, item in enumerate(worker_specs_payload, start=1)
        ]
    else:
        worker_specs = default_worker_specs(
            _coerce_int(payload.get("worker_count"), DEFAULT_WORKER_COUNT),
            _coerce_int(payload.get("worker_start_port"), DEFAULT_WORKER_START_PORT),
            _coerce_int(payload.get("worker_concurrency"), DEFAULT_WORKER_CONCURRENCY),
        )
    templates = [
        TaskTemplate(
            task_type=str(item.get("task_type", "sleep")),
            count=_coerce_int(item.get("count"), 0),
            payload=str(item.get("payload", "")),
            priority=_coerce_int(item.get("priority"), 0),
            name_prefix=str(item.get("name_prefix", "")).strip(),
        )
        for item in payload.get("task_templates", [])
    ]
    templates = [template for template in templates if template.count > 0]

    worker_count = len(worker_specs)
    config = ClusterConfig(
        scheduler_host=str(payload.get("scheduler_host") or DEFAULT_SCHEDULER_HOST).strip(),
        scheduler_port=_coerce_int(payload.get("scheduler_port"), DEFAULT_SCHEDULER_PORT),
        strategy=str(payload.get("strategy") or "weighted_score").strip(),
        worker_count=worker_count,
        worker_start_port=worker_specs[0].listen_port if worker_specs else DEFAULT_WORKER_START_PORT,
        worker_concurrency=worker_specs[0].max_concurrent_tasks if worker_specs else DEFAULT_WORKER_CONCURRENCY,
        worker_specs=worker_specs,
        task_templates=templates,
        task_limit=_coerce_int(payload.get("task_limit"), DEFAULT_TASK_LIST_LIMIT),
        start_workers_first=bool(payload.get("start_workers_first", False)),
        use_existing_scheduler=bool(payload.get("use_existing_scheduler", False)),
    )
    validate_config(config)
    return config


def config_from_args(args: argparse.Namespace) -> ClusterConfig:
    worker_specs = default_worker_specs(args.workers, args.worker_start_port, args.worker_concurrency)
    config = ClusterConfig(
        scheduler_host=args.scheduler_host,
        scheduler_port=args.scheduler_port,
        strategy=args.strategy,
        worker_count=len(worker_specs),
        worker_start_port=worker_specs[0].listen_port if worker_specs else args.worker_start_port,
        worker_concurrency=worker_specs[0].max_concurrent_tasks if worker_specs else args.worker_concurrency,
        worker_specs=worker_specs,
        task_templates=default_task_templates(args.tasks),
        task_limit=args.task_limit,
        start_workers_first=args.start_workers_first,
        use_existing_scheduler=args.use_existing_scheduler,
    )
    validate_config(config)
    return config


def validate_config(config: ClusterConfig) -> None:
    if config.strategy not in ALLOWED_STRATEGIES:
        raise ValueError(f"Unsupported strategy: {config.strategy}. Choose one of {', '.join(ALLOWED_STRATEGIES)}.")
    if not config.scheduler_host:
        raise ValueError("scheduler_host cannot be empty.")
    _validate_port(config.scheduler_port, "scheduler_port")
    if not 1 <= config.task_limit <= 5000:
        raise ValueError("task_limit must be between 1 and 5000.")
    if not config.task_templates:
        raise ValueError("At least one task template with count > 0 is required.")
    worker_specs = config.resolved_worker_specs()
    if not worker_specs:
        raise ValueError("At least one worker specification is required.")
    if len(worker_specs) > 50:
        raise ValueError("worker count must be between 1 and 50.")
    seen_worker_ids: set[str] = set()
    seen_ports: set[int] = set()
    for index, worker_spec in enumerate(worker_specs, start=1):
        validate_worker_spec(worker_spec, index)
        if worker_spec.worker_id in seen_worker_ids:
            raise ValueError(f"Duplicate worker_id: {worker_spec.worker_id}.")
        if worker_spec.listen_port in seen_ports:
            raise ValueError(f"Duplicate worker listen_port: {worker_spec.listen_port}.")
        seen_worker_ids.add(worker_spec.worker_id)
        seen_ports.add(worker_spec.listen_port)
    for template in config.task_templates:
        if template.normalized_type() not in TASK_TYPES:
            raise ValueError(f"Unsupported task_type: {template.task_type}.")
        if not 1 <= template.count <= 5000:
            raise ValueError("Each task template count must be between 1 and 5000.")
        if not str(template.payload).strip():
            raise ValueError(f"Task template payload cannot be empty: {template.task_type}.")
        validate_task_payload(template)


def validate_task_payload(template: TaskTemplate) -> None:
    task_type = template.normalized_type()
    payload = str(template.payload).strip()
    if task_type == "sleep":
        try:
            seconds = float(payload)
        except ValueError as exc:
            raise ValueError("sleep task payload must be a number of seconds.") from exc
        if seconds < 0:
            raise ValueError("sleep task payload cannot be negative.")
        return
    if task_type == "fibonacci":
        try:
            number = int(payload)
        except ValueError as exc:
            raise ValueError("fibonacci task payload must be an integer n.") from exc
        if number < 0:
            raise ValueError("fibonacci task payload cannot be negative.")


def worker_spec_from_payload(item: dict[str, Any], index: int) -> WorkerSpec:
    worker_id = str(item.get("worker_id") or f"worker-{index}").strip()
    listen_port = _coerce_int(item.get("listen_port"), 0)
    max_concurrent_tasks = _coerce_int(item.get("max_concurrent_tasks"), DEFAULT_WORKER_CONCURRENCY)
    return WorkerSpec(worker_id=worker_id, listen_port=listen_port, max_concurrent_tasks=max_concurrent_tasks)


def validate_worker_spec(worker_spec: WorkerSpec, index: int) -> None:
    if not worker_spec.worker_id:
        raise ValueError(f"worker_specs[{index}] worker_id cannot be empty.")
    _validate_port(worker_spec.listen_port, f"worker_specs[{index}].listen_port")
    if not 1 <= worker_spec.max_concurrent_tasks <= 64:
        raise ValueError(f"worker_specs[{index}].max_concurrent_tasks must be between 1 and 64.")


def _validate_port(value: int, name: str) -> None:
    if not 1 <= value <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535.")


def _coerce_int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def make_request_handler(
    controller: DemoClusterController,
    default_config: ClusterConfig,
) -> type[BaseHTTPRequestHandler]:
    class DashboardRequestHandler(BaseHTTPRequestHandler):
        server_version = "DemoClusterDashboard/1.0"

        def do_GET(self) -> None:  # noqa: N802 - stdlib hook name
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                self._send_html(DASHBOARD_HTML)
                return
            if path == "/api/config":
                self._send_json({"default_config": default_config.to_dict(), "strategies": list(ALLOWED_STRATEGIES)})
                return
            if path == "/api/state":
                self._send_json(controller.snapshot())
                return
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802 - stdlib hook name
            path = urlparse(self.path).path
            try:
                if path == "/api/start":
                    payload = self._read_json()
                    config = config_from_payload(payload)
                    accepted, message = controller.start_async(config)
                    status = HTTPStatus.ACCEPTED if accepted else HTTPStatus.CONFLICT
                    self._send_json({"accepted": accepted, "message": message, "state": controller.snapshot()}, status=status)
                    return
                if path == "/api/stop":
                    controller.stop()
                    self._send_json({"accepted": True, "state": controller.snapshot()})
                    return
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
            logging.debug("HTTP: " + format, *args)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            body = self.rfile.read(length)
            return json.loads(body.decode("utf-8"))

        def _send_html(self, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return DashboardRequestHandler


def create_dashboard_server(host: str, port: int, handler: type[BaseHTTPRequestHandler]) -> DashboardHTTPServer:
    try:
        return DashboardHTTPServer((host, port), handler)
    except OSError:
        if port == 0:
            raise
        logging.warning("Dashboard port %s is unavailable; asking the OS for a free port.", port)
        return DashboardHTTPServer((host, 0), handler)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the visual demo cluster dashboard.")
    parser.add_argument("--scheduler-host", default=DEFAULT_SCHEDULER_HOST, help="Default Scheduler host shown in the UI.")
    parser.add_argument("--scheduler-port", type=int, default=DEFAULT_SCHEDULER_PORT, help="Default Scheduler port shown in the UI.")
    parser.add_argument("--strategy", default="weighted_score", choices=ALLOWED_STRATEGIES, help="Default load-balancing strategy.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKER_COUNT, help="Default Worker count shown in the UI.")
    parser.add_argument("--worker-start-port", type=int, default=DEFAULT_WORKER_START_PORT, help="Default first Worker listen port.")
    parser.add_argument("--worker-concurrency", type=int, default=DEFAULT_WORKER_CONCURRENCY, help="Default max concurrent tasks per Worker.")
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASK_COUNT, help="Default total task count split across the UI task templates.")
    parser.add_argument("--task-limit", type=int, default=DEFAULT_TASK_LIST_LIMIT, help="Maximum number of recent tasks rendered in the UI.")
    parser.add_argument("--start-workers-first", action="store_true", help="Prefill UI to start Workers before submitting tasks.")
    parser.add_argument("--use-existing-scheduler", action="store_true", help="Prefill UI to connect to an existing Scheduler.")
    parser.add_argument("--dashboard-host", default=DEFAULT_DASHBOARD_HOST, help="Dashboard HTTP bind host.")
    parser.add_argument("--dashboard-port", type=int, default=DEFAULT_DASHBOARD_PORT, help="Dashboard HTTP bind port.")
    parser.add_argument("--no-open-browser", action="store_true", help="Do not open the dashboard in the default browser.")
    parser.add_argument("--auto-start", action="store_true", help="Start the cluster immediately with the default UI values.")
    parser.add_argument("--headless", action="store_true", help="Run the demo cluster without the browser dashboard.")
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging("dashboard")
    args = build_arg_parser().parse_args(argv)
    default_config = config_from_args(args)
    controller = DemoClusterController(PROJECT_ROOT)

    if args.headless:
        return run_headless(controller, default_config)

    handler = make_request_handler(controller, default_config)
    server = create_dashboard_server(args.dashboard_host, args.dashboard_port, handler)
    host, port = server.server_address[:2]
    display_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    url = f"http://{display_host}:{port}/"

    print(f"Dashboard URL: {url}", flush=True)
    print("Use the web UI to configure scheduler, workers, task templates, and live monitoring.", flush=True)

    if args.auto_start:
        accepted, message = controller.start_async(default_config)
        print(message if accepted else f"Cluster not started: {message}", flush=True)

    if not args.no_open_browser:
        try:
            webbrowser.open_new_tab(url)
        except Exception as exc:  # noqa: BLE001 - browser opening is best-effort.
            logging.warning("Could not open browser automatically: %s", exc)

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nStopping dashboard and demo cluster...")
    finally:
        server.server_close()
        controller.stop()
    return 0


def run_headless(controller: DemoClusterController, config: ClusterConfig) -> int:
    try:
        controller.start_blocking(config)
        print(f"Demo cluster is running at scheduler={config.scheduler_address}. Press Ctrl+C to stop.")
        while True:
            exited = controller.exited_processes()
            if exited:
                for process in exited:
                    print(f"Child process exited: name={process['name']} pid={process['pid']} exit_code={process['exit_code']}")
                return 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping demo cluster...")
        return 0
    finally:
        controller.stop()


DASHBOARD_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Distributed Scheduler Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f8fa;
      --panel: #ffffff;
      --text: #1f2328;
      --muted: #656d76;
      --line: #d0d7de;
      --accent: #0969da;
      --accent-strong: #0550ae;
      --green: #1a7f37;
      --orange: #bc4c00;
      --red: #cf222e;
      --purple: #8250df;
      --gray: #6e7781;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      padding: 16px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      position: sticky;
      top: 0;
      z-index: 3;
    }
    h1, h2 {
      margin: 0;
      line-height: 1.2;
      letter-spacing: 0;
    }
    h1 { font-size: 18px; }
    h2 { font-size: 15px; }
    main {
      width: min(1480px, calc(100vw - 32px));
      margin: 16px auto 32px;
      display: grid;
      grid-template-columns: minmax(360px, 460px) minmax(0, 1fr);
      gap: 16px;
    }
    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      overflow: hidden;
    }
    .stack { display: grid; gap: 16px; align-content: start; }
    .grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .grid-3 { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .field { display: grid; gap: 4px; }
    label { color: var(--muted); font-size: 12px; }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      border-radius: 6px;
      padding: 7px 8px;
      font: inherit;
      min-height: 34px;
    }
    textarea { resize: vertical; min-height: 52px; }
    input[type="checkbox"] { width: auto; min-height: 0; }
    .check {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text);
      padding-top: 18px;
    }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }
    button {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 12px;
      background: #fff;
      color: var(--text);
      font: inherit;
      cursor: pointer;
      min-height: 36px;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: white;
    }
    button.primary:hover { background: var(--accent-strong); }
    button.danger {
      color: var(--red);
      border-color: #ff8182;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: .55;
    }
    .muted { color: var(--muted); }
    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
    }
    .status-line {
      display: flex;
      align-items: center;
      gap: 8px;
      white-space: nowrap;
      color: var(--muted);
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
      background: var(--gray);
      flex: 0 0 auto;
    }
    .dot.running, .dot.healthy, .dot.succeeded { background: var(--green); }
    .dot.starting, .dot.queued { background: var(--orange); }
    .dot.error, .dot.failed, .dot.dead { background: var(--red); }
    .dot.stopping, .dot.timed_out { background: var(--purple); }
    .metrics {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fbfbfc;
      min-width: 0;
    }
    .metric strong {
      display: block;
      font-size: 20px;
      line-height: 1.1;
      margin-top: 4px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 8px;
      text-align: left;
      vertical-align: middle;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      background: #f6f8fa;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .table-wrap {
      max-height: 430px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .task-table { max-height: 520px; }
    .bar {
      width: 100%;
      height: 8px;
      background: #eaeef2;
      border-radius: 999px;
      overflow: hidden;
      margin-top: 4px;
    }
    .fill {
      height: 100%;
      width: 0%;
      background: var(--accent);
    }
    .fill.cpu { background: var(--orange); }
    .fill.mem { background: var(--purple); }
    .fill.load { background: var(--green); }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      color: var(--muted);
      max-width: 100%;
    }
    .task-row {
      display: grid;
      grid-template-columns: 118px 70px 1fr 76px 90px 36px;
      gap: 8px;
      align-items: end;
      padding: 8px 0;
      border-bottom: 1px solid var(--line);
    }
    .task-row:last-child { border-bottom: 0; }
    .worker-row {
      display: grid;
      grid-template-columns: 128px 110px 120px 1fr 1fr 36px;
      gap: 8px;
      align-items: end;
      padding: 8px 0;
      border-bottom: 1px solid var(--line);
    }
    .worker-row:last-child { border-bottom: 0; }
    .worker-task-list {
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }
    .mini-title {
      color: var(--muted);
      font-size: 12px;
      margin: 0 0 4px;
    }
    .worker-panel {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #fbfbfc;
    }
    .stat-list {
      display: grid;
      gap: 4px;
    }
    .stat-list .line {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      min-width: 0;
    }
    .stat-list .label {
      color: var(--muted);
      white-space: nowrap;
    }
    .stat-list .value {
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .message {
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfbfc;
      color: var(--muted);
      min-height: 42px;
    }
    .message.error {
      border-color: #ff8182;
      color: var(--red);
      background: #fff5f5;
    }
    @media (max-width: 1100px) {
      main { grid-template-columns: 1fr; }
      .metrics { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 720px) {
      header { align-items: flex-start; flex-direction: column; }
      main { width: calc(100vw - 20px); }
      .grid-2, .grid-3 { grid-template-columns: 1fr; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .task-row { grid-template-columns: 1fr 1fr; }
      .worker-row { grid-template-columns: 1fr 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>分布式任务调度可视化控制台</h1>
      <div class="muted">Scheduler / Worker / Task 参数配置与实时状态监控</div>
    </div>
    <div class="status-line">
      <span id="phase-dot" class="dot"></span>
      <span id="phase-text">加载中</span>
      <span id="last-refresh"></span>
    </div>
  </header>

  <main>
    <div class="stack">
      <section>
        <div class="toolbar">
          <h2>Scheduler</h2>
          <span id="scheduler-health" class="chip">未连接</span>
        </div>
        <div class="grid-2">
          <div class="field">
            <label for="scheduler-host">监听地址</label>
            <input id="scheduler-host" value="127.0.0.1" />
          </div>
          <div class="field">
            <label for="scheduler-port">端口</label>
            <input id="scheduler-port" type="number" min="1" max="65535" />
          </div>
          <div class="field">
            <label for="strategy">负载均衡策略</label>
            <select id="strategy">
              <option value="weighted_score">weighted_score</option>
              <option value="least_loaded">least_loaded</option>
              <option value="round_robin">round_robin</option>
            </select>
          </div>
          <label class="check">
            <input id="use-existing-scheduler" type="checkbox" />
            连接已有 Scheduler
          </label>
        </div>
      </section>

      <section>
        <div class="toolbar">
          <h2>Worker</h2>
          <span class="muted">逐个配置 Worker 并发和端口</span>
        </div>
        <div id="worker-rows"></div>
        <div class="actions">
          <button id="add-worker-row" type="button">添加 Worker</button>
        </div>
        <label class="check">
          <input id="start-workers-first" type="checkbox" />
          先启动 Worker 再提交任务
        </label>
      </section>

      <section>
        <div class="toolbar">
          <h2>Task 批次</h2>
          <button id="add-task-row" type="button">添加模板</button>
        </div>
        <div id="task-rows"></div>
        <div class="grid-2" style="margin-top: 10px;">
          <div class="field">
            <label for="task-limit">任务表显示上限</label>
            <input id="task-limit" type="number" min="1" max="5000" />
          </div>
          <div class="field">
            <label>模板任务总数</label>
            <input id="task-total" disabled />
          </div>
        </div>
        <div class="actions">
          <button id="start-button" class="primary" type="button">启动集群</button>
          <button id="stop-button" class="danger" type="button">停止集群</button>
          <button id="refresh-button" type="button">立即刷新</button>
        </div>
      </section>

      <section>
        <div class="toolbar">
          <h2>运行消息</h2>
        </div>
        <div id="message" class="message">等待状态刷新。</div>
      </section>
    </div>

    <div class="stack">
      <section>
        <div class="toolbar">
          <h2>实时概览</h2>
          <span id="scheduler-address" class="muted"></span>
        </div>
        <div class="metrics">
          <div class="metric"><span class="muted">Worker 存活</span><strong id="metric-workers">0/0</strong></div>
          <div class="metric"><span class="muted">任务总数</span><strong id="metric-total">0</strong></div>
          <div class="metric"><span class="muted">排队</span><strong id="metric-queued">0</strong></div>
          <div class="metric"><span class="muted">运行中</span><strong id="metric-running">0</strong></div>
          <div class="metric"><span class="muted">已完成</span><strong id="metric-completed">0</strong></div>
          <div class="metric"><span class="muted">失败/超时</span><strong id="metric-failed">0</strong></div>
        </div>
      </section>

      <section>
        <div class="toolbar">
          <h2>Worker 负载</h2>
          <span class="muted">CPU / Memory / 并发占用 / 任务统计</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th style="width: 128px;">Worker</th>
                <th style="width: 78px;">状态</th>
                <th>地址</th>
                <th style="width: 112px;">并发</th>
                <th style="width: 120px;">CPU</th>
                <th style="width: 120px;">内存</th>
                <th style="width: 90px;">分数</th>
                <th style="width: 100px;">心跳</th>
              </tr>
            </thead>
            <tbody id="workers-body"></tbody>
          </table>
        </div>
      </section>

      <section>
        <div class="toolbar">
          <h2>Worker 任务统计</h2>
          <span class="muted">统计运行数量、任务名、耗时和最近执行记录</span>
        </div>
        <div id="worker-stats" class="worker-task-list"></div>
      </section>

      <section>
        <div class="toolbar">
          <h2>任务执行情况</h2>
          <span id="task-limit-note" class="muted"></span>
        </div>
        <div class="table-wrap task-table">
          <table>
            <thead>
              <tr>
                <th style="width: 108px;">ID</th>
                <th style="width: 190px;">名称</th>
                <th style="width: 98px;">类型</th>
                <th style="width: 110px;">状态</th>
                <th style="width: 120px;">Worker</th>
                <th style="width: 88px;">负载</th>
                <th style="width: 92px;">耗时</th>
                <th>结果 / 错误</th>
              </tr>
            </thead>
            <tbody id="tasks-body"></tbody>
          </table>
        </div>
      </section>

      <section>
        <div class="toolbar">
          <h2>本地进程</h2>
          <span class="muted">Dashboard 启动的 Scheduler 和 Worker</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th style="width: 130px;">名称</th>
                <th style="width: 90px;">类型</th>
                <th style="width: 100px;">PID</th>
                <th style="width: 90px;">状态</th>
                <th>日志</th>
              </tr>
            </thead>
            <tbody id="processes-body"></tbody>
          </table>
        </div>
      </section>
    </div>
  </main>

  <script>
    const ids = {
      schedulerHost: document.getElementById('scheduler-host'),
      schedulerPort: document.getElementById('scheduler-port'),
      strategy: document.getElementById('strategy'),
      useExistingScheduler: document.getElementById('use-existing-scheduler'),
      workerCount: document.getElementById('worker-count'),
      workerConcurrency: document.getElementById('worker-concurrency'),
      workerStartPort: document.getElementById('worker-start-port'),
      startWorkersFirst: document.getElementById('start-workers-first'),
      workerRows: document.getElementById('worker-rows'),
      taskRows: document.getElementById('task-rows'),
      taskLimit: document.getElementById('task-limit'),
      taskTotal: document.getElementById('task-total'),
      startButton: document.getElementById('start-button'),
      stopButton: document.getElementById('stop-button'),
      refreshButton: document.getElementById('refresh-button'),
      addTaskRow: document.getElementById('add-task-row'),
      addWorkerRow: document.getElementById('add-worker-row'),
      message: document.getElementById('message'),
      phaseDot: document.getElementById('phase-dot'),
      phaseText: document.getElementById('phase-text'),
      lastRefresh: document.getElementById('last-refresh'),
      schedulerHealth: document.getElementById('scheduler-health'),
      schedulerAddress: document.getElementById('scheduler-address'),
      workersBody: document.getElementById('workers-body'),
      workerStats: document.getElementById('worker-stats'),
      tasksBody: document.getElementById('tasks-body'),
      processesBody: document.getElementById('processes-body'),
      taskLimitNote: document.getElementById('task-limit-note'),
      metrics: {
        workers: document.getElementById('metric-workers'),
        total: document.getElementById('metric-total'),
        queued: document.getElementById('metric-queued'),
        running: document.getElementById('metric-running'),
        completed: document.getElementById('metric-completed'),
        failed: document.getElementById('metric-failed'),
      },
    };

    const phaseText = {
      idle: '空闲',
      starting: '启动中',
      running: '运行中',
      stopping: '停止中',
      error: '错误'
    };

    const placeholders = {
      sleep: '秒数，例如 3',
      fibonacci: 'n，例如 32',
      word_count: '文本负载，例如 distributed scheduler demo'
    };

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, options);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || data.message || response.statusText);
      }
      return data;
    }

    function hydrateConfig(config) {
      ids.schedulerHost.value = config.scheduler_host;
      ids.schedulerPort.value = config.scheduler_port;
      ids.strategy.value = config.strategy;
      ids.useExistingScheduler.checked = Boolean(config.use_existing_scheduler);
      ids.workerRows.innerHTML = '';
      const workerSpecs = config.worker_specs && config.worker_specs.length
        ? config.worker_specs
        : defaultWorkerSpecs(config);
      workerSpecs.forEach(addWorkerRow);
      ids.startWorkersFirst.checked = Boolean(config.start_workers_first);
      ids.taskLimit.value = config.task_limit;
      ids.taskRows.innerHTML = '';
      config.task_templates.forEach(addTaskRow);
      updateTaskTotal();
    }

    function addWorkerRow(spec = {}) {
      const row = document.createElement('div');
      row.className = 'worker-row';
      row.innerHTML = `
        <div class="field">
          <label>Worker ID</label>
          <input data-field="worker_id" />
        </div>
        <div class="field">
          <label>监听端口</label>
          <input data-field="listen_port" type="number" min="1" max="65535" />
        </div>
        <div class="field">
          <label>并发数量</label>
          <input data-field="max_concurrent_tasks" type="number" min="1" max="64" />
        </div>
        <div class="field">
          <label>说明</label>
          <input data-field="hint" disabled />
        </div>
        <button type="button" data-action="remove">移除</button>
      `;
      row.querySelector('[data-field="worker_id"]').value = spec.worker_id || `worker-${ids.workerRows.children.length + 1}`;
      row.querySelector('[data-field="listen_port"]').value = spec.listen_port ?? 50061 + ids.workerRows.children.length;
      row.querySelector('[data-field="max_concurrent_tasks"]').value = spec.max_concurrent_tasks ?? 2;
      row.querySelector('[data-field="hint"]').value = '每个 Worker 单独配置';
      row.querySelector('[data-action="remove"]').addEventListener('click', () => {
        row.remove();
        updateWorkerSummary();
      });
      row.querySelector('[data-field="worker_id"]').addEventListener('input', updateWorkerSummary);
      row.querySelector('[data-field="listen_port"]').addEventListener('input', updateWorkerSummary);
      row.querySelector('[data-field="max_concurrent_tasks"]').addEventListener('input', updateWorkerSummary);
      ids.workerRows.appendChild(row);
      updateWorkerSummary();
    }

    function addTaskRow(template = {}) {
      const row = document.createElement('div');
      row.className = 'task-row';
      row.innerHTML = `
        <div class="field">
          <label>类型</label>
          <select data-field="task_type">
            <option value="sleep">sleep</option>
            <option value="fibonacci">fibonacci</option>
            <option value="word_count">word_count</option>
          </select>
        </div>
        <div class="field">
          <label>数量</label>
          <input data-field="count" type="number" min="0" max="5000" />
        </div>
        <div class="field">
          <label>负载 / payload</label>
          <input data-field="payload" />
        </div>
        <div class="field">
          <label>优先级</label>
          <input data-field="priority" type="number" />
        </div>
        <div class="field">
          <label>名称前缀</label>
          <input data-field="name_prefix" />
        </div>
        <button type="button" data-action="remove">移除</button>
      `;
      const type = row.querySelector('[data-field="task_type"]');
      const payload = row.querySelector('[data-field="payload"]');
      type.value = template.task_type || 'sleep';
      row.querySelector('[data-field="count"]').value = template.count ?? 1;
      payload.value = template.payload || '';
      row.querySelector('[data-field="priority"]').value = template.priority ?? 0;
      row.querySelector('[data-field="name_prefix"]').value = template.name_prefix || '';
      payload.placeholder = placeholders[type.value] || '';
      type.addEventListener('change', () => {
        payload.placeholder = placeholders[type.value] || '';
      });
      row.querySelector('[data-action="remove"]').addEventListener('click', () => {
        row.remove();
        updateTaskTotal();
      });
      row.querySelector('[data-field="count"]').addEventListener('input', updateTaskTotal);
      ids.taskRows.appendChild(row);
      updateTaskTotal();
    }

    function collectConfig() {
      const workerSpecs = Array.from(ids.workerRows.querySelectorAll('.worker-row')).map(row => ({
        worker_id: row.querySelector('[data-field="worker_id"]').value.trim(),
        listen_port: Number(row.querySelector('[data-field="listen_port"]').value || 0),
        max_concurrent_tasks: Number(row.querySelector('[data-field="max_concurrent_tasks"]').value || 0),
      })).filter(item => item.worker_id);
      const taskTemplates = Array.from(ids.taskRows.querySelectorAll('.task-row')).map(row => ({
        task_type: row.querySelector('[data-field="task_type"]').value,
        count: Number(row.querySelector('[data-field="count"]').value || 0),
        payload: row.querySelector('[data-field="payload"]').value.trim(),
        priority: Number(row.querySelector('[data-field="priority"]').value || 0),
        name_prefix: row.querySelector('[data-field="name_prefix"]').value.trim()
      })).filter(item => item.count > 0);

      return {
        scheduler_host: ids.schedulerHost.value.trim(),
        scheduler_port: Number(ids.schedulerPort.value),
        strategy: ids.strategy.value,
        use_existing_scheduler: ids.useExistingScheduler.checked,
        worker_specs: workerSpecs,
        worker_count: workerSpecs.length,
        worker_concurrency: workerSpecs[0]?.max_concurrent_tasks || 1,
        worker_start_port: workerSpecs[0]?.listen_port || 50061,
        start_workers_first: ids.startWorkersFirst.checked,
        task_limit: Number(ids.taskLimit.value),
        task_templates: taskTemplates
      };
    }

    function updateTaskTotal() {
      const total = Array.from(ids.taskRows.querySelectorAll('[data-field="count"]'))
        .reduce((sum, input) => sum + Number(input.value || 0), 0);
      ids.taskTotal.value = total;
    }

    function defaultWorkerSpecs(config) {
      const count = Math.max(Number(config.worker_count || 1), 1);
      const startPort = Number(config.worker_start_port || 50061);
      const concurrency = Math.max(Number(config.worker_concurrency || 1), 1);
      return Array.from({length: count}, (_, index) => ({
        worker_id: `worker-${index + 1}`,
        listen_port: startPort + index,
        max_concurrent_tasks: concurrency,
      }));
    }

    function updateWorkerSummary() {
      const rows = Array.from(ids.workerRows.querySelectorAll('.worker-row'));
      const workerIds = rows.map(row => row.querySelector('[data-field="worker_id"]').value.trim()).filter(Boolean);
      const ports = rows.map(row => Number(row.querySelector('[data-field="listen_port"]').value || 0));
      const valid = rows.length > 0
        && workerIds.length === rows.length
        && new Set(workerIds).size === workerIds.length
        && new Set(ports).size === ports.length
        && ports.every(port => port > 0);
      ids.startButton.disabled = !valid;
    }

    async function startCluster() {
      setMessage('正在发送启动请求...');
      try {
        const data = await fetchJson('/api/start', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(collectConfig())
        });
        setMessage(data.message || '启动请求已接受。');
        renderState(data.state);
      } catch (error) {
        setMessage(error.message, true);
      }
    }

    async function stopCluster() {
      setMessage('正在停止集群...');
      try {
        const data = await fetchJson('/api/stop', {method: 'POST'});
        renderState(data.state);
      } catch (error) {
        setMessage(error.message, true);
      }
    }

    async function refreshState() {
      try {
        const data = await fetchJson('/api/state');
        renderState(data);
      } catch (error) {
        setMessage(error.message, true);
      }
    }

    function renderState(data) {
      const phase = data.phase || 'idle';
      ids.phaseDot.className = `dot ${phase}`;
      ids.phaseText.textContent = phaseText[phase] || phase;
      ids.lastRefresh.textContent = `刷新 ${new Date().toLocaleTimeString()}`;
      ids.startButton.disabled = ['starting', 'running', 'stopping'].includes(phase);
      ids.stopButton.disabled = phase === 'idle';

      setMessage(data.message || '');
      if (data.last_error) {
        setMessage(data.last_error, true);
      }

      const scheduler = data.scheduler || {};
      ids.schedulerAddress.textContent = scheduler.address || '';
      ids.schedulerHealth.innerHTML = scheduler.healthy
        ? '<span class="dot healthy"></span> Scheduler 正常'
        : '<span class="dot dead"></span> Scheduler 未就绪';

      const summary = data.summary || {};
      ids.metrics.workers.textContent = `${summary.alive_workers || 0}/${summary.configured_workers || 0}`;
      ids.metrics.total.textContent = summary.total_tasks || 0;
      ids.metrics.queued.textContent = summary.queued || 0;
      ids.metrics.running.textContent = summary.running || 0;
      ids.metrics.completed.textContent = summary.completed || 0;
      ids.metrics.failed.textContent = (summary.failed || 0) + (summary.timed_out || 0);

      ids.taskLimitNote.textContent = `显示最近 ${summary.visible_task_limit || 0} 条，已提交 ${summary.submitted_tasks || 0}/${summary.expected_tasks || 0}`;
      renderWorkers(data.workers || []);
      renderWorkerStats(data.worker_stats || []);
      renderTasks(data.tasks || []);
      renderProcesses(data.processes || []);
    }

    function renderWorkers(workers) {
      if (!workers.length) {
        ids.workersBody.innerHTML = '<tr><td colspan="8" class="muted">暂无 Worker 心跳。</td></tr>';
        return;
      }
      ids.workersBody.innerHTML = workers.map(worker => `
        <tr>
          <td title="${escapeHtml(worker.worker_id)}">${escapeHtml(worker.worker_id)}</td>
          <td><span class="chip"><span class="dot ${worker.alive ? 'healthy' : 'dead'}"></span>${worker.alive ? 'alive' : 'dead'}</span></td>
          <td title="${escapeHtml(worker.address)}">${escapeHtml(worker.address)}</td>
          <td>${worker.running_tasks}/${worker.max_concurrent_tasks}<div class="bar"><div class="fill load" style="width:${clamp(worker.concurrency_percent)}%"></div></div></td>
          <td>${worker.cpu_percent.toFixed(1)}%<div class="bar"><div class="fill cpu" style="width:${clamp(worker.cpu_percent)}%"></div></div></td>
          <td>${worker.memory_percent.toFixed(1)}%<div class="bar"><div class="fill mem" style="width:${clamp(worker.memory_percent)}%"></div></div></td>
          <td>${worker.load_score.toFixed(1)}</td>
          <td>${worker.heartbeat_age_seconds.toFixed(1)}s</td>
        </tr>
      `).join('');
    }

    function renderWorkerStats(workerStats) {
      if (!workerStats.length) {
        ids.workerStats.innerHTML = '<div class="muted">暂无 Worker 任务统计。</div>';
        return;
      }
      ids.workerStats.innerHTML = workerStats.map(worker => {
        const recentTasks = (worker.task_summaries || []).map(task => `
          <div class="line">
            <span class="label">${escapeHtml(task.short_task_id)} ${escapeHtml(task.name)}</span>
            <span class="value">${escapeHtml(task.status)} ${formatDuration(task.duration_ms)}</span>
          </div>
        `).join('');
        const taskNames = (worker.task_names || []).map(name => `<span class="chip">${escapeHtml(name)}</span>`).join('');
        return `
          <div class="worker-panel">
            <div class="toolbar">
              <h2 style="font-size:14px;">${escapeHtml(worker.worker_id)}</h2>
              <span class="chip">${worker.alive ? 'alive' : 'dead'}</span>
            </div>
            <div class="grid-3" style="margin-bottom:10px;">
              <div class="metric"><span class="muted">运行任务</span><strong>${worker.total_tasks || 0}</strong></div>
              <div class="metric"><span class="muted">总耗时</span><strong>${formatDuration(worker.total_duration_ms)}</strong></div>
              <div class="metric"><span class="muted">平均耗时</span><strong>${formatDuration(worker.average_duration_ms)}</strong></div>
            </div>
            <div class="stat-list">
              <div class="line"><span class="label">配置并发</span><span class="value">${worker.configured_max_concurrent_tasks || 0}</span></div>
              <div class="line"><span class="label">当前运行</span><span class="value">${worker.current_running_tasks || 0}</span></div>
              <div class="line"><span class="label">已完成</span><span class="value">${worker.succeeded_tasks || 0}</span></div>
              <div class="line"><span class="label">失败/超时</span><span class="value">${(worker.failed_tasks || 0) + (worker.timed_out_tasks || 0)}</span></div>
            </div>
            <div style="margin-top:10px;">
              <div class="mini-title">运行过的任务</div>
              <div class="worker-task-list">${taskNames || '<div class="muted">暂无任务记录。</div>'}</div>
            </div>
            <div style="margin-top:10px;">
              <div class="mini-title">最近执行</div>
              <div class="worker-task-list">${recentTasks || '<div class="muted">暂无任务记录。</div>'}</div>
            </div>
          </div>
        `;
      }).join('');
    }

    function renderTasks(tasks) {
      if (!tasks.length) {
        ids.tasksBody.innerHTML = '<tr><td colspan="8" class="muted">暂无任务。</td></tr>';
        return;
      }
      ids.tasksBody.innerHTML = tasks.map(task => {
        const info = task.error || task.result || task.payload || '';
        return `
          <tr>
            <td title="${escapeHtml(task.task_id)}">${escapeHtml(task.short_task_id)}</td>
            <td title="${escapeHtml(task.name)}">${escapeHtml(task.name)}</td>
            <td>${escapeHtml(task.task_type)}</td>
            <td><span class="chip"><span class="dot ${task.status}"></span>${escapeHtml(task.status)}</span></td>
            <td title="${escapeHtml(task.assigned_worker_id || '-')}">${escapeHtml(task.assigned_worker_id || '-')}</td>
            <td title="${escapeHtml(task.payload)}">${escapeHtml(shorten(task.payload, 18))}</td>
            <td>${formatDuration(task.duration_ms)}</td>
            <td title="${escapeHtml(info)}">${escapeHtml(shorten(info, 80))}</td>
          </tr>
        `;
      }).join('');
    }

    function renderProcesses(processes) {
      if (!processes.length) {
        ids.processesBody.innerHTML = '<tr><td colspan="5" class="muted">暂无本地子进程。</td></tr>';
        return;
      }
      ids.processesBody.innerHTML = processes.map(process => `
        <tr>
          <td>${escapeHtml(process.name)}</td>
          <td>${escapeHtml(process.kind)}</td>
          <td>${process.pid}</td>
          <td><span class="chip"><span class="dot ${process.alive ? 'healthy' : 'dead'}"></span>${process.alive ? 'running' : 'exit ' + process.exit_code}</span></td>
          <td title="${escapeHtml(process.log_path)}">${escapeHtml(process.log_path)}</td>
        </tr>
      `).join('');
    }

    function setMessage(message, isError = false) {
      ids.message.textContent = message || '等待状态刷新。';
      ids.message.className = `message ${isError ? 'error' : ''}`;
    }

    function clamp(value) {
      return Math.max(0, Math.min(100, Number(value) || 0));
    }

    function formatDuration(ms) {
      if (!ms) return '-';
      if (ms < 1000) return `${ms}ms`;
      return `${(ms / 1000).toFixed(1)}s`;
    }

    function shorten(value, maxLen) {
      const text = String(value || '');
      return text.length > maxLen ? text.slice(0, maxLen - 1) + '…' : text;
    }

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, char => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      }[char]));
    }

    ids.addTaskRow.addEventListener('click', () => addTaskRow({task_type: 'sleep', count: 1, payload: '3', priority: 0}));
    ids.addWorkerRow.addEventListener('click', () => addWorkerRow({worker_id: `worker-${ids.workerRows.children.length + 1}`, listen_port: 50061 + ids.workerRows.children.length, max_concurrent_tasks: 2}));
    ids.startButton.addEventListener('click', startCluster);
    ids.stopButton.addEventListener('click', stopCluster);
    ids.refreshButton.addEventListener('click', refreshState);

    (async function init() {
      try {
        const config = await fetchJson('/api/config');
        hydrateConfig(config.default_config);
      } catch (error) {
        setMessage(error.message, true);
      }
      await refreshState();
      setInterval(refreshState, 1500);
    })();
  </script>
</body>
</html>
"""
