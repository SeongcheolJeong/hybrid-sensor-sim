#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate canonical_lane_graph_v0 map.")
    parser.add_argument("--map", required=True, help="Canonical map JSON path")
    parser.add_argument("--report-out", default="", help="Optional report JSON path")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    from hybrid_sensor_sim.maps import build_canonical_map_validation_report, load_map_payload

    try:
        args = _parse_args(argv)
        map_path = Path(args.map).resolve()
        report = build_canonical_map_validation_report(load_map_payload(map_path, "canonical map"), map_path=map_path)
        if args.report_out:
            report_path = Path(args.report_out).resolve()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
            print(f"[ok] report={report_path}")
        if report["error_count"]:
            for item in report["errors"]:
                print(f"[error] {item}")
            for item in report["warnings"]:
                print(f"[warn] {item}")
            return 1
        for item in report["warnings"]:
            print(f"[warn] {item}")
        print("[ok] canonical map validation passed")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] run_map_validate.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
