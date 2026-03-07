from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest, SensorSimResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run AWSIM/CARLA renderer smoke execution with automatic output inspection."
    )
    parser.add_argument("--config", type=Path, required=True, help="Base JSON config file.")
    parser.add_argument(
        "--backend",
        choices=("awsim", "carla"),
        required=True,
        help="Renderer backend to smoke.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Override output directory for the smoke run.",
    )
    parser.add_argument(
        "--backend-bin",
        help="Override direct backend executable path for the selected backend.",
    )
    parser.add_argument(
        "--renderer-map",
        help="Override renderer map/town value for the smoke run.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_smoke_summary.json. Defaults under output_dir.",
    )
    parser.add_argument(
        "--set-option",
        action="append",
        default=[],
        help="Override option values using dotted keys, for example camera_projection_enabled=true or camera_intrinsics.fx=1200.",
    )
    return parser.parse_args(argv)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Config payload must be a JSON object.")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _resolve_runtime_path(raw: Any) -> Path:
    path = Path(str(raw)).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _parse_option_override(raw: str) -> tuple[str, Any]:
    if "=" not in raw:
        raise ValueError(f"Invalid --set-option value: {raw}")
    key, value_text = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"Invalid --set-option value: {raw}")
    try:
        value = json.loads(value_text)
    except json.JSONDecodeError:
        value = value_text
    return key, value


def _set_dotted_option(options: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [part.strip() for part in dotted_key.split(".") if part.strip()]
    if not parts:
        raise ValueError(f"Invalid option key: {dotted_key}")
    cursor = options
    for part in parts[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            child = {}
            cursor[part] = child
        cursor = child
    cursor[parts[-1]] = value


def _load_artifact_payload(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _build_comparison_table(
    output_comparison_report: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(output_comparison_report, dict):
        return None
    raw_by_sensor = output_comparison_report.get("by_sensor")
    if not isinstance(raw_by_sensor, list):
        return None

    sensor_rows: list[dict[str, Any]] = []
    role_rows: list[dict[str, Any]] = []
    sensor_status_counts: dict[str, int] = {}
    role_status_counts: dict[str, int] = {}
    mismatch_reason_counts: dict[str, int] = {}
    for sensor_entry in raw_by_sensor:
        if not isinstance(sensor_entry, dict):
            continue
        sensor_status = str(sensor_entry.get("status", "")).strip()
        if sensor_status:
            sensor_status_counts[sensor_status] = sensor_status_counts.get(sensor_status, 0) + 1
        sensor_mismatch_reasons = [
            str(item).strip()
            for item in sensor_entry.get("mismatch_reasons", [])
            if str(item).strip()
        ]
        for reason in sensor_mismatch_reasons:
            mismatch_reason_counts[reason] = mismatch_reason_counts.get(reason, 0) + 1
        sensor_rows.append(
            {
                "sensor_id": str(sensor_entry.get("sensor_id", "")).strip(),
                "sensor_name": str(sensor_entry.get("sensor_name", "")).strip(),
                "status": sensor_status,
                "mismatch_reasons": sensor_mismatch_reasons,
                "found_output_roles": [
                    str(item).strip()
                    for item in sensor_entry.get("found_output_roles", [])
                    if str(item).strip()
                ],
                "missing_output_roles": [
                    str(item).strip()
                    for item in sensor_entry.get("missing_output_roles", [])
                    if str(item).strip()
                ],
                "found_output_count": sensor_entry.get("found_output_count"),
                "missing_output_count": sensor_entry.get("missing_output_count"),
                "unexpected_output_count": sensor_entry.get("unexpected_output_count"),
            }
        )
        raw_role_diffs = sensor_entry.get("role_diffs")
        if not isinstance(raw_role_diffs, list):
            continue
        sensor_id = str(sensor_entry.get("sensor_id", "")).strip()
        for role_entry in raw_role_diffs:
            if not isinstance(role_entry, dict):
                continue
            role_status = str(role_entry.get("status", "")).strip()
            if role_status:
                role_status_counts[role_status] = role_status_counts.get(role_status, 0) + 1
            role_mismatch_reasons = [
                str(item).strip()
                for item in role_entry.get("mismatch_reasons", [])
                if str(item).strip()
            ]
            for reason in role_mismatch_reasons:
                mismatch_reason_counts[reason] = mismatch_reason_counts.get(reason, 0) + 1
            role_rows.append(
                {
                    "sensor_id": sensor_id,
                    "output_role": str(role_entry.get("output_role", "")).strip(),
                    "status": role_status,
                    "mismatch_reasons": role_mismatch_reasons,
                    "found_output_count": role_entry.get("found_output_count"),
                    "missing_output_count": role_entry.get("missing_output_count"),
                    "expected_backend_filenames": [
                        str(item).strip()
                        for item in role_entry.get("expected_backend_filenames", [])
                        if str(item).strip()
                    ],
                    "discovered_backend_filenames": [
                        str(item).strip()
                        for item in role_entry.get("discovered_backend_filenames", [])
                        if str(item).strip()
                    ],
                    "found_relative_paths": [
                        str(item).strip()
                        for item in role_entry.get("found_relative_paths", [])
                        if str(item).strip()
                    ],
                    "missing_relative_paths": [
                        str(item).strip()
                        for item in role_entry.get("missing_relative_paths", [])
                        if str(item).strip()
                    ],
                }
            )
    return {
        "sensor_rows": sensor_rows,
        "role_rows": role_rows,
        "sensor_status_counts": sensor_status_counts,
        "role_status_counts": role_status_counts,
        "mismatch_reason_counts": mismatch_reason_counts,
    }


def _build_effective_config(
    *,
    base_config: dict[str, Any],
    backend: str,
    output_dir_override: Path | None,
    backend_bin_override: str | None,
    renderer_map_override: str | None,
    option_overrides: list[str],
    repo_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    effective = dict(base_config)
    options = dict(effective.get("options", {}))
    forced_options: dict[str, Any] = {
        "renderer_bridge_enabled": True,
        "renderer_backend": backend,
        "renderer_execute": True,
        "renderer_execute_via_runner": True,
        "renderer_execute_and_inspect_via_runner": True,
        "renderer_fail_on_error": True,
        "renderer_cwd": str(repo_root),
        f"{backend}_wrapper": str((repo_root / "scripts" / f"renderer_launch_{backend}.sh").resolve()),
    }
    if backend_bin_override:
        forced_options[f"{backend}_bin"] = str(_resolve_runtime_path(backend_bin_override))
    if renderer_map_override is not None:
        forced_options["renderer_map"] = renderer_map_override

    for key, value in forced_options.items():
        options[key] = value
    for raw_override in option_overrides:
        dotted_key, value = _parse_option_override(raw_override)
        _set_dotted_option(options, dotted_key, value)

    effective["options"] = options
    effective["scenario_path"] = str(_resolve_runtime_path(effective["scenario_path"]))
    if output_dir_override is not None:
        effective["output_dir"] = str(_resolve_runtime_path(output_dir_override))
    elif "output_dir" in effective:
        effective["output_dir"] = str(_resolve_runtime_path(effective["output_dir"]))
    if effective.get("helios_bin"):
        effective["helios_bin"] = str(_resolve_runtime_path(effective["helios_bin"]))
    return effective, forced_options


def build_renderer_backend_smoke_summary(
    *,
    result: SensorSimResult,
    config_path: Path,
    effective_config_path: Path,
    backend: str,
    forced_options: dict[str, Any],
) -> dict[str, Any]:
    artifact_paths = {key: str(path) for key, path in result.artifacts.items()}
    backend_run_manifest = _load_artifact_payload(result.artifacts.get("backend_run_manifest"))
    pipeline_summary = _load_artifact_payload(result.artifacts.get("renderer_pipeline_summary"))
    output_inspection_manifest = _load_artifact_payload(
        result.artifacts.get("backend_output_inspection_manifest")
    )
    runner_smoke_manifest = _load_artifact_payload(
        result.artifacts.get("backend_runner_smoke_manifest")
    )
    output_smoke_report = _load_artifact_payload(result.artifacts.get("backend_output_smoke_report"))
    output_comparison_report = _load_artifact_payload(
        result.artifacts.get("backend_output_comparison_report")
    )
    comparison_table = _build_comparison_table(output_comparison_report)

    summary = {
        "config_path": str(config_path),
        "effective_config_path": str(effective_config_path),
        "backend": backend,
        "result_backend": result.backend,
        "success": result.success,
        "message": result.message,
        "forced_options": forced_options,
        "artifacts": artifact_paths,
        "metrics": result.metrics,
        "run": {
            "status": backend_run_manifest.get("status") if isinstance(backend_run_manifest, dict) else None,
            "failure_reason": (
                backend_run_manifest.get("failure_reason")
                if isinstance(backend_run_manifest, dict)
                else None
            ),
            "return_code": (
                backend_run_manifest.get("return_code")
                if isinstance(backend_run_manifest, dict)
                else None
            ),
        },
        "pipeline": {
            "status": pipeline_summary.get("status") if isinstance(pipeline_summary, dict) else None,
            "success": pipeline_summary.get("success") if isinstance(pipeline_summary, dict) else None,
        },
        "output_inspection": (
            {
                "status": output_inspection_manifest.get("status"),
                "success": output_inspection_manifest.get("success"),
                "return_code": output_inspection_manifest.get("return_code"),
                "message": output_inspection_manifest.get("message"),
            }
            if isinstance(output_inspection_manifest, dict)
            else None
        ),
        "runner_smoke": (
            {
                "status": runner_smoke_manifest.get("status"),
                "success": runner_smoke_manifest.get("success"),
                "return_code": runner_smoke_manifest.get("return_code"),
                "message": runner_smoke_manifest.get("message"),
            }
            if isinstance(runner_smoke_manifest, dict)
            else None
        ),
        "output_smoke_report": (
            {
                "status": output_smoke_report.get("status"),
                "coverage_ratio": output_smoke_report.get("coverage_ratio"),
            }
            if isinstance(output_smoke_report, dict)
            else None
        ),
        "output_comparison": (
            {
                "status": output_comparison_report.get("status"),
                "mismatch_reasons": output_comparison_report.get("mismatch_reasons", []),
                "unexpected_output_count": output_comparison_report.get(
                    "unexpected_output_count"
                ),
            }
            if isinstance(output_comparison_report, dict)
            else None
        ),
        "comparison_table": comparison_table,
    }
    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    config_path = args.config.expanduser().resolve()
    base_config = _load_json(config_path)
    effective_config, forced_options = _build_effective_config(
        base_config=base_config,
        backend=args.backend,
        output_dir_override=args.output_dir,
        backend_bin_override=args.backend_bin,
        renderer_map_override=args.renderer_map,
        option_overrides=list(args.set_option),
        repo_root=repo_root,
    )
    output_dir = _resolve_runtime_path(effective_config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    effective_config_path = output_dir / "renderer_backend_smoke_config.json"
    _write_json(effective_config_path, effective_config)

    mode = BackendMode(effective_config.get("mode", BackendMode.HYBRID_AUTO.value))
    options = dict(effective_config.get("options", {}))
    if effective_config.get("helios_runtime") and "helios_runtime" not in options:
        options["helios_runtime"] = effective_config["helios_runtime"]
    request = SensorSimRequest(
        scenario_path=Path(effective_config["scenario_path"]),
        output_dir=output_dir,
        sensor_profile=effective_config.get("sensor_profile", "default"),
        seed=int(effective_config.get("seed", 0)),
        options=options,
    )
    orchestrator = HybridOrchestrator(
        helios=HeliosAdapter(
            helios_bin=(
                Path(effective_config["helios_bin"])
                if effective_config.get("helios_bin")
                else None
            )
        ),
        native=NativePhysicsBackend(),
    )
    result = orchestrator.run(request, mode)
    summary = build_renderer_backend_smoke_summary(
        result=result,
        config_path=config_path,
        effective_config_path=effective_config_path,
        backend=args.backend,
        forced_options=forced_options,
    )
    summary_path = (
        _resolve_runtime_path(args.summary_path)
        if args.summary_path is not None
        else output_dir / "renderer_backend_smoke_summary.json"
    )
    _write_json(summary_path, summary)
    print(json.dumps(summary, indent=2))
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
