#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_pythonpath() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    src_text = str(src_root)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)


def main() -> int:
    _bootstrap_pythonpath()
    from hybrid_sensor_sim.tools.scenario_runtime_backend_rebridge import main as rebridge_main

    return int(rebridge_main())


if __name__ == "__main__":
    raise SystemExit(main())
