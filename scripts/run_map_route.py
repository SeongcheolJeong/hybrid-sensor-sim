#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute shortest route on canonical_lane_graph_v0.")
    parser.add_argument("--map", required=True, help="Canonical map JSON path")
    parser.add_argument("--entry-lane-id", default="", help="Optional explicit entry lane id")
    parser.add_argument("--exit-lane-id", default="", help="Optional explicit exit lane id")
    parser.add_argument("--via-lane-id", action="append", default=[], help="Optional via lane id (repeatable)")
    parser.add_argument("--cost-mode", choices=["hops", "length"], default="hops")
    parser.add_argument("--report-out", default="", help="Optional route report output path")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    from hybrid_sensor_sim.maps import load_and_compute_canonical_route

    try:
        args = _parse_args(argv)
        report = load_and_compute_canonical_route(
            Path(args.map).resolve(),
            entry_lane_id=args.entry_lane_id,
            exit_lane_id=args.exit_lane_id,
            via_lane_ids=[str(item) for item in args.via_lane_id],
            cost_mode=args.cost_mode,
        )
        if args.report_out:
            report_path = Path(args.report_out).resolve()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
            print(f"[ok] report={report_path}")
        print("[ok] route_status=pass")
        print(
            f"[ok] route={report['selected_entry_lane_id']}->{report['selected_exit_lane_id']} "
            f"lanes={report['route_lane_count']} hops={report['route_hop_count']} cost_mode={report['route_cost_mode']}"
        )
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] run_map_route.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
