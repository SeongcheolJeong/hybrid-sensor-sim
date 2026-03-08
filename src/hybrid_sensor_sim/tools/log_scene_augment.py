from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios.log_scene import load_log_scene


def augment_log_scene(
    payload: dict[str, Any],
    *,
    ego_speed_scale: float,
    lead_gap_offset_m: float,
    lead_speed_offset_mps: float,
    suffix: str,
) -> dict[str, Any]:
    normalized = load_log_scene(payload)
    result = dict(normalized)
    result["log_id"] = f"{normalized['log_id']}_{suffix}"
    result["ego_initial_speed_mps"] = float(normalized["ego_initial_speed_mps"]) * ego_speed_scale
    result["lead_vehicle_initial_gap_m"] = (
        float(normalized["lead_vehicle_initial_gap_m"]) + lead_gap_offset_m
    )
    result["lead_vehicle_speed_mps"] = (
        float(normalized["lead_vehicle_speed_mps"]) + lead_speed_offset_mps
    )
    result["augmentation"] = {
        "ego_speed_scale": ego_speed_scale,
        "lead_gap_offset_m": lead_gap_offset_m,
        "lead_speed_offset_mps": lead_speed_offset_mps,
        "source_log_id": normalized["log_id"],
    }
    return result


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Augment a log_scene_v0 payload.")
    parser.add_argument("--input", required=True, help="Input log_scene_v0 JSON path")
    parser.add_argument("--out", required=True, help="Output augmented log scene JSON path")
    parser.add_argument("--ego-speed-scale", type=float, default=1.0, help="Scale factor for ego speed")
    parser.add_argument("--lead-gap-offset-m", type=float, default=0.0, help="Offset added to lead gap")
    parser.add_argument("--lead-speed-offset-mps", type=float, default=0.0, help="Offset added to lead speed")
    parser.add_argument("--suffix", default="aug", help="Suffix appended to log_id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        input_path = Path(args.input).resolve()
        out_path = Path(args.out).resolve()
        payload = load_log_scene(input_path)
        augmented = augment_log_scene(
            payload,
            ego_speed_scale=float(args.ego_speed_scale),
            lead_gap_offset_m=float(args.lead_gap_offset_m),
            lead_speed_offset_mps=float(args.lead_speed_offset_mps),
            suffix=str(args.suffix),
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(augmented, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        print(f"[ok] input={input_path}")
        print(f"[ok] out={out_path}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] log_scene_augment.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
