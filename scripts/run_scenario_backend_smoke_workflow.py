#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_src_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


def main() -> int:
    _bootstrap_src_path()
    from hybrid_sensor_sim.tools.scenario_backend_smoke_workflow import (
        main as scenario_backend_smoke_workflow_main,
    )

    return scenario_backend_smoke_workflow_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
