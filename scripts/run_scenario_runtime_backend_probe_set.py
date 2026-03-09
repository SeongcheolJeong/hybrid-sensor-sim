#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_src_path() -> None:
    script_path = Path(__file__).resolve()
    repo_root = script_path.parents[1]
    src_root = repo_root / "src"
    src_text = str(src_root)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)


def main() -> int:
    _bootstrap_src_path()
    from hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set import (
        main as tool_main,
    )

    return tool_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
