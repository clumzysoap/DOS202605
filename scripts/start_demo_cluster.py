"""Start the visual demo cluster dashboard.

Default usage:

    python scripts/start_demo_cluster.py

The command opens a local browser dashboard where Scheduler, Worker, and task
batch parameters can be configured before the cluster is started.

Useful options:

    python scripts/start_demo_cluster.py --dashboard-port 8765
    python scripts/start_demo_cluster.py --workers 5 --worker-concurrency 2 --tasks 200
    python scripts/start_demo_cluster.py --auto-start
    python scripts/start_demo_cluster.py --headless
"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from distributed_scheduler.cluster_dashboard import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
