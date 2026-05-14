"""生成 gRPC Python 代码。

这个脚本封装了 ``grpc_tools.protoc`` 命令，避免手动输入较长的生成命令。

运行方式：

    python scripts/generate_grpc.py

生成结果：

    distributed_scheduler/generated/task_scheduler_pb2.py
    distributed_scheduler/generated/task_scheduler_pb2_grpc.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROTO_FILE = PROJECT_ROOT / "proto" / "task_scheduler.proto"
OUTPUT_DIR = PROJECT_ROOT / "distributed_scheduler" / "generated"


def main() -> int:
    """运行 grpc_tools.protoc 生成代码。"""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROJECT_ROOT / 'proto'}",
        f"--python_out={OUTPUT_DIR}",
        f"--grpc_python_out={OUTPUT_DIR}",
        str(PROTO_FILE),
    ]

    print("正在生成 gRPC Python 文件...")
    print(" ".join(command))

    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    if completed.returncode != 0:
        print("生成失败：请确认已安装 requirements.txt 中的 grpcio-tools。", file=sys.stderr)
        return completed.returncode

    _patch_generated_import()
    print(f"生成完成，输出目录：{OUTPUT_DIR}")
    return 0


def _patch_generated_import() -> None:
    """修正生成文件中的导入路径。

    grpc_tools 默认生成的 ``task_scheduler_pb2_grpc.py`` 会写成
    ``import task_scheduler_pb2 as task__scheduler__pb2``。由于本项目把生成文件放在
    ``distributed_scheduler.generated`` 包内，需要改成相对导入。
    """

    grpc_file = OUTPUT_DIR / "task_scheduler_pb2_grpc.py"
    if not grpc_file.exists():
        return

    text = grpc_file.read_text(encoding="utf-8")
    text = text.replace(
        "import task_scheduler_pb2 as task__scheduler__pb2",
        "from . import task_scheduler_pb2 as task__scheduler__pb2",
    )
    grpc_file.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
