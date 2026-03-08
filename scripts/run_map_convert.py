#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert map formats between simple_map_v0 and canonical_lane_graph_v0.")
    parser.add_argument("--input", required=True, help="Input JSON path")
    parser.add_argument("--out", required=True, help="Output JSON path")
    parser.add_argument("--to-format", required=True, choices=["canonical", "simple"], help="Target format")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    from hybrid_sensor_sim.maps import convert_map_payload, load_map_payload

    try:
        args = _parse_args(argv)
        input_path = Path(args.input).resolve()
        out_path = Path(args.out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        converted = convert_map_payload(load_map_payload(input_path, "input"), to_format=args.to_format)
        out_path.write_text(json.dumps(converted, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        print(f"[ok] to_format={args.to_format}")
        print(f"[ok] out={out_path}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] run_map_convert.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
