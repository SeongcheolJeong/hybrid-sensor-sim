#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    from hybrid_sensor_sim.tools.autonomy_e2e_history_refresh import main as refresh_main

    return refresh_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
