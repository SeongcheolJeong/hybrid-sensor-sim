from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from hybrid_sensor_sim.scenarios.log_scene import load_log_scene
from hybrid_sensor_sim.scenarios.replay import build_replay_manifest, build_scenario_from_log_scene
from hybrid_sensor_sim.tools.object_sim_runner import run_object_sim_job


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay a log_scene_v0 payload through object sim.")
    parser.add_argument("--log-scene", required=True, help="Log scene JSON path")
    parser.add_argument("--run-id", required=True, help="Replay run ID")
    parser.add_argument("--out", required=True, help="Output root directory")
    parser.add_argument("--seed", default="", help="Deterministic seed for replay")
    parser.add_argument("--sds-version", default="sds_unknown", help="SDS version identifier")
    parser.add_argument("--sim-version", default="sim_engine_v0_prototype", help="Sim version identifier")
    parser.add_argument("--fidelity-profile", default="dev-fast", help="Fidelity profile")
    return parser.parse_args(argv)


def _parse_int(raw: str, *, default: int, field: str) -> int:
    value = str(raw).strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer, got: {raw}") from exc


def run_log_replay(
    *,
    log_scene_path: Path,
    run_id: str,
    out_root: Path,
    seed: int,
    sds_version: str,
    sim_version: str,
    fidelity_profile: str,
) -> dict[str, object]:
    log_scene = load_log_scene(log_scene_path)
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    scenario_payload = build_scenario_from_log_scene(log_scene)
    scenario_path = run_dir / "replay_scenario.json"
    scenario_path.write_text(json.dumps(scenario_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    job = run_object_sim_job(
        scenario_path=scenario_path,
        run_id=run_id,
        out_root=out_root,
        seed=seed,
        metadata={
            "run_source": "log_replay_closed_loop",
            "sds_version": sds_version,
            "sim_version": sim_version,
            "fidelity_profile": fidelity_profile,
            "map_id": str(log_scene["map_id"]),
            "map_version": str(log_scene["map_version"]),
            "odd_tags": [],
            "batch_id": None,
        },
    )
    manifest = build_replay_manifest(
        log_scene_path=str(log_scene_path.resolve()),
        log_id=str(log_scene["log_id"]),
        run_id=run_id,
        scenario_path=str(scenario_path.resolve()),
        summary_path=str(job["summary_path"]),
        status=str(job["summary"]["status"]),
        termination_reason=str(job["summary"]["termination_reason"]),
    )
    manifest_path = run_dir / "log_replay_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return {
        "scenario_path": scenario_path,
        "summary_path": job["summary_path"],
        "trace_path": job["trace_path"],
        "lane_risk_summary_path": job["lane_risk_summary_path"],
        "manifest_path": manifest_path,
        "summary": job["summary"],
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        log_scene_path = Path(args.log_scene).resolve()
        out_root = Path(args.out).resolve()
        out_root.mkdir(parents=True, exist_ok=True)
        seed = _parse_int(args.seed, default=42, field="seed")
        result = run_log_replay(
            log_scene_path=log_scene_path,
            run_id=args.run_id,
            out_root=out_root,
            seed=seed,
            sds_version=args.sds_version,
            sim_version=args.sim_version,
            fidelity_profile=args.fidelity_profile,
        )
        print(f"[ok] run_id={args.run_id}")
        print(f"[ok] scenario={result['scenario_path']}")
        print(f"[ok] summary={result['summary_path']}")
        print(f"[ok] manifest={result['manifest_path']}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] log_replay_runner.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
