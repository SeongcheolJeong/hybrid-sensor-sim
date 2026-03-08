from __future__ import annotations

import argparse
import contextlib
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.renderer_backend_workflow import (
    build_renderer_backend_workflow,
)

_BACKEND_EXECUTABLE_NAMES = {
    "awsim": "AWSIM-Demo.x86_64",
    "carla": "CarlaUnreal.sh",
}
_BACKEND_MAP_ENV_KEYS = {
    "awsim": "AWSIM_RENDERER_MAP",
    "carla": "CARLA_RENDERER_MAP",
}
_BACKEND_DEFAULT_MAPS = {
    "awsim": "SampleMap",
    "carla": "Town03",
}
_ARCHIVE_SOURCE_CHOICES = ("local_candidate", "download_url")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run an end-to-end package workflow self-test using a synthetic packaged backend, "
            "local acquire/stage, and backend smoke execution."
        )
    )
    parser.add_argument(
        "--backend",
        choices=("awsim", "carla"),
        default="awsim",
        help="Backend flavor to synthesize for the package workflow self-test.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/renderer_backend_package_workflow_selftest"),
        help="Directory where self-test artifacts will be written.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_package_workflow_selftest.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--archive-source",
        choices=_ARCHIVE_SOURCE_CHOICES,
        default="local_candidate",
        help="Whether acquire should reuse a local archive candidate or download via a file:// URL.",
    )
    return parser.parse_args(argv)


def _resolve_path(raw: str | Path) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_fake_helios_script(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    out="$2"
    shift 2
  else
    shift
  fi
done
mkdir -p "${out}/demo/2026-01-01_00-00-00"
rootdir="${out}/demo/2026-01-01_00-00-00"
echo "Output directory: \\"${rootdir}\\""
cat > "${rootdir}/scan_points.xyz" <<EOF
10.0 0.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
1.0 0.0 0.0 1.0 0.0 0.0 0.0
EOF
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _fake_backend_archive_script() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["BACKEND_OUTPUT_SPEC_PATH"]).read_text(encoding="utf-8"))
for entry in spec.get("expected_outputs", []):
    path = Path(entry["path"])
    if entry.get("kind") == "directory":
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"artifact_key": entry["artifact_key"]}), encoding="utf-8")
PY
"""


def _write_fake_backend_archive(*, backend: str, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    executable_name = _BACKEND_EXECUTABLE_NAMES[backend]
    with zipfile.ZipFile(archive_path, "w") as handle:
        handle.writestr(
            f"{backend}-demo/{executable_name}",
            _fake_backend_archive_script(),
        )


def _write_base_config(
    *,
    workspace_root: Path,
    survey_path: Path,
    helios_bin: Path,
    output_dir: Path,
) -> Path:
    config_path = workspace_root / "base_config.json"
    config_path.write_text(
        json.dumps(
            {
                "mode": "hybrid_auto",
                "helios_runtime": "binary",
                "helios_bin": str(helios_bin),
                "scenario_path": str(survey_path),
                "output_dir": str(output_dir),
                "sensor_profile": "smoke",
                "seed": 17,
                "options": {
                    "helios_runtime": "binary",
                    "execute_helios": True,
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": False,
                    "renderer_backend": "none",
                    "renderer_execute": False,
                    "renderer_fail_on_error": False,
                    "renderer_command": [],
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def _build_setup_summary(
    *,
    backend: str,
    workspace_root: Path,
    setup_summary_path: Path,
    helios_bin: Path,
    archive_path: Path,
    archive_source: str,
) -> dict[str, Any]:
    map_key = _BACKEND_MAP_ENV_KEYS[backend]
    payload: dict[str, Any] = {
        "generated_at_utc": _format_utc(_utc_now()),
        "search_roots": [str(workspace_root.resolve())],
        "selection": {
            "HELIOS_BIN": str(helios_bin.resolve()),
            map_key: _BACKEND_DEFAULT_MAPS[backend],
        },
        "readiness": {
            "helios_binary_ready": True,
            "helios_binary_host_compatible": True,
            "helios_docker_ready": False,
            "helios_ready": True,
        },
        "acquisition_hints": {
            backend: {
                "platform_supported": True,
                "platform_note": "Synthetic self-test runtime.",
            }
        },
        "artifacts": {
            "summary_path": str(setup_summary_path),
        },
    }
    backend_hints = payload["acquisition_hints"][backend]
    if archive_source == "local_candidate":
        backend_hints["local_download_candidates"] = [str(archive_path.resolve())]
    else:
        backend_hints["download_options"] = [
            {
                "name": archive_path.name,
                "url": archive_path.resolve().as_uri(),
            }
        ]
    _write_json(setup_summary_path, payload)
    return payload


def run_renderer_backend_package_workflow_selftest(
    *,
    backend: str = "awsim",
    output_root: Path,
    summary_path: Path | None = None,
    archive_source: str = "local_candidate",
) -> dict[str, Any]:
    backend = backend.strip().lower()
    if backend not in _BACKEND_EXECUTABLE_NAMES:
        raise ValueError(f"Unsupported backend: {backend}")
    archive_source = archive_source.strip().lower()
    if archive_source not in _ARCHIVE_SOURCE_CHOICES:
        raise ValueError(f"Unsupported archive_source: {archive_source}")

    output_root = _resolve_path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = (
        _resolve_path(summary_path)
        if summary_path is not None
        else (output_root / "renderer_backend_package_workflow_selftest.json").resolve()
    )

    workspace_root = output_root / "workspace"
    inputs_root = workspace_root / "inputs"
    downloads_root = workspace_root / "Downloads"
    source_archives_root = workspace_root / "source_archives"
    workflow_root = output_root / "workflow_run"
    smoke_base_output = output_root / "smoke_base_output"
    survey_path = inputs_root / "survey.xml"
    survey_path.parent.mkdir(parents=True, exist_ok=True)
    survey_path.write_text("<document></document>", encoding="utf-8")
    helios_bin = inputs_root / "fake_helios.sh"
    _write_fake_helios_script(helios_bin)

    if archive_source == "local_candidate":
        archive_path = downloads_root / f"{backend.upper()}-Demo.zip"
    else:
        archive_path = source_archives_root / f"{backend.upper()}-Demo.zip"
    _write_fake_backend_archive(backend=backend, archive_path=archive_path)

    setup_summary_path = output_root / "seed_setup" / "renderer_backend_local_setup.json"
    seeded_setup_summary = _build_setup_summary(
        backend=backend,
        workspace_root=workspace_root,
        setup_summary_path=setup_summary_path,
        helios_bin=helios_bin,
        archive_path=archive_path,
        archive_source=archive_source,
    )
    config_path = _write_base_config(
        workspace_root=workspace_root,
        survey_path=survey_path,
        helios_bin=helios_bin,
        output_dir=smoke_base_output,
    )

    with contextlib.redirect_stdout(io.StringIO()):
        workflow_summary = build_renderer_backend_workflow(
            backend=backend,
            repo_root=workspace_root,
            workflow_root=workflow_root,
            setup_summary_path=setup_summary_path,
            config_path=config_path,
            auto_acquire=True,
            download_dir=downloads_root,
        )

    workflow_summary_path = workflow_root / "renderer_backend_workflow_summary.json"
    _write_json(workflow_summary_path, workflow_summary)

    output_comparison_status = (
        workflow_summary.get("smoke", {})
        .get("summary", {})
        .get("output_comparison", {})
        .get("status")
    )
    final_selection = workflow_summary.get("final_selection", {})
    backend_bin_key = "AWSIM_BIN" if backend == "awsim" else "CARLA_BIN"
    payload = {
        "generated_at_utc": _format_utc(_utc_now()),
        "backend": backend,
        "archive_source": archive_source,
        "output_root": str(output_root),
        "summary_path": str(summary_path),
        "workspace_root": str(workspace_root),
        "seeded_setup_summary_path": str(setup_summary_path),
        "seeded_setup_summary": seeded_setup_summary,
        "archive_path": str(archive_path),
        "archive_exists": archive_path.exists(),
        "config_path": str(config_path),
        "workflow_root": str(workflow_root),
        "workflow_summary_path": str(workflow_summary_path),
        "workflow_status": workflow_summary.get("status"),
        "workflow_success": workflow_summary.get("success"),
        "output_comparison_status": output_comparison_status,
        "acquire_stage_ready": bool(
            workflow_summary.get("acquire", {})
            .get("readiness", {})
            .get("stage_ready")
        ),
        "staged_backend_bin": final_selection.get(backend_bin_key),
        "workflow": workflow_summary,
        "success": bool(
            workflow_summary.get("status") == "SMOKE_SUCCEEDED"
            and workflow_summary.get("success")
            and output_comparison_status == "MATCHED"
            and workflow_summary.get("smoke", {}).get("executed")
            and workflow_summary.get("refreshed_setup") is not None
            and final_selection.get(backend_bin_key)
        ),
    }
    _write_json(summary_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    payload = run_renderer_backend_package_workflow_selftest(
        backend=args.backend,
        output_root=args.output_root,
        summary_path=args.summary_path,
        archive_source=args.archive_source,
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
