from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.renderer_backend_workflow import (
    main as workflow_main,
)

_BACKEND_EXECUTABLE_NAMES = {
    "awsim": "AWSIM-Demo.x86_64",
    "carla": "CarlaUnreal.sh",
}
_BACKEND_DEFAULT_MAPS = {
    "awsim": "SampleMap",
    "carla": "Town03",
}
_DEFAULT_HELIOS_DOCKER_IMAGE = "heliosplusplus:cli"
_DEFAULT_HELIOS_DOCKER_BINARY = "/home/jovyan/helios/build/helios++"
_DEFAULT_STALE_OFFSET_SECONDS = 2 * 60 * 60


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run an end-to-end renderer backend workflow self-test using a synthetic host-incompatible "
            "backend runtime and the Linux handoff Docker path."
        )
    )
    parser.add_argument(
        "--backend",
        choices=("awsim", "carla"),
        default="awsim",
        help="Backend flavor to synthesize for the workflow self-test.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="Repo root containing configs/ and scripts/. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/renderer_backend_workflow_selftest"),
        help="Directory where self-test artifacts will be written.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_workflow_selftest.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker CLI binary used for the workflow Docker handoff path.",
    )
    parser.add_argument(
        "--docker-image",
        default="python:3.11-slim",
        help="Linux Docker image used for the workflow Docker handoff path.",
    )
    parser.add_argument(
        "--docker-container-workspace",
        default="/workspace",
        help="Container workspace used for the workflow Docker handoff path.",
    )
    parser.add_argument(
        "--docker-handoff-execute",
        action="store_true",
        help="Execute the extracted handoff script inside Docker instead of verify-only mode.",
    )
    parser.add_argument(
        "--preflight-max-age-seconds",
        type=int,
        default=60,
        help="Maximum age for the stale cached preflight inserted into the synthetic setup summary.",
    )
    parser.add_argument(
        "--stale-preflight-offset-seconds",
        type=int,
        default=_DEFAULT_STALE_OFFSET_SECONDS,
        help="How old the seeded cached preflight should be before workflow refreshes it.",
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


def _elf_stub_bytes() -> bytes:
    return bytes(
        [
            0x7F,
            0x45,
            0x4C,
            0x46,
            0x02,
            0x01,
            0x01,
            0x00,
        ]
    ) + (b"\x00" * 56)


def _create_incompatible_backend_stub(*, backend: str, search_root: Path) -> Path:
    executable_name = _BACKEND_EXECUTABLE_NAMES[backend]
    path = search_root / executable_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_elf_stub_bytes())
    path.chmod(0o755)
    return path


def _build_stale_setup_summary(
    *,
    backend: str,
    repo_root: Path,
    output_root: Path,
    backend_bin_path: Path,
    stale_offset_seconds: int,
    docker_image: str,
) -> tuple[dict[str, Any], Path]:
    setup_root = output_root / "seed_setup"
    setup_root.mkdir(parents=True, exist_ok=True)
    summary_path = setup_root / "renderer_backend_local_setup.json"
    map_key = "AWSIM_RENDERER_MAP" if backend == "awsim" else "CARLA_RENDERER_MAP"
    bin_key = "AWSIM_BIN" if backend == "awsim" else "CARLA_BIN"
    preflight_summary_path = setup_root / "linux_handoff_docker_selftest_probe" / "renderer_backend_linux_handoff_selftest.json"
    stale_generated_at = _format_utc(_utc_now() - timedelta(seconds=stale_offset_seconds))
    payload = {
        "generated_at_utc": _format_utc(_utc_now()),
        "search_roots": [str(backend_bin_path.parent.resolve())],
        "selection": {
            "HELIOS_DOCKER_IMAGE": _DEFAULT_HELIOS_DOCKER_IMAGE,
            "HELIOS_DOCKER_BINARY": _DEFAULT_HELIOS_DOCKER_BINARY,
            bin_key: str(backend_bin_path.resolve()),
            map_key: _BACKEND_DEFAULT_MAPS[backend],
        },
        "readiness": {
            "helios_ready": True,
            f"{backend}_host_compatible": False,
        },
        "probes": {
            "linux_handoff_docker_selftest": {
                "success": True,
                "execute": False,
                "marker_exists": False,
                "generated_at_utc": stale_generated_at,
                "docker": {
                    "return_code": 0,
                    "docker_image": docker_image,
                },
                "summary_path": str(preflight_summary_path),
            }
        },
        "commands": {
            "linux_handoff_docker_selftest": (
                "python3 scripts/discover_renderer_backend_local_env.py "
                "--probe-linux-handoff-docker-selftest"
            ),
        },
        "artifacts": {
            "summary_path": str(summary_path),
            "env_path": str(setup_root / "renderer_backend_local.env.sh"),
            "linux_handoff_docker_selftest_probe_path": str(preflight_summary_path),
        },
    }
    _write_json(summary_path, payload)
    return payload, summary_path


def run_renderer_backend_workflow_selftest(
    *,
    backend: str = "awsim",
    repo_root: Path | None = None,
    output_root: Path,
    summary_path: Path | None = None,
    docker_binary: str = "docker",
    docker_image: str = "python:3.11-slim",
    docker_container_workspace: str = "/workspace",
    docker_handoff_execute: bool = False,
    preflight_max_age_seconds: int = 60,
    stale_preflight_offset_seconds: int = _DEFAULT_STALE_OFFSET_SECONDS,
) -> dict[str, Any]:
    backend = backend.strip().lower()
    if backend not in _BACKEND_EXECUTABLE_NAMES:
        raise ValueError(f"Unsupported backend: {backend}")
    repo_root = _resolve_path(repo_root) if repo_root is not None else Path.cwd().resolve()
    output_root = _resolve_path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = (
        _resolve_path(summary_path)
        if summary_path is not None
        else (output_root / "renderer_backend_workflow_selftest.json").resolve()
    )

    inputs_root = output_root / "inputs"
    backend_bin_path = _create_incompatible_backend_stub(backend=backend, search_root=inputs_root)
    seeded_setup_summary, setup_summary_path = _build_stale_setup_summary(
        backend=backend,
        repo_root=repo_root,
        output_root=output_root,
        backend_bin_path=backend_bin_path,
        stale_offset_seconds=stale_preflight_offset_seconds,
        docker_image=docker_image,
    )

    workflow_root = output_root / "workflow_run"
    workflow_args = [
        "--backend",
        backend,
        "--setup-summary",
        str(setup_summary_path),
        "--dry-run",
        "--run-linux-handoff-docker",
        "--refresh-docker-handoff-preflight",
        "--docker-binary",
        docker_binary,
        "--docker-image",
        docker_image,
        "--docker-container-workspace",
        docker_container_workspace,
        "--docker-handoff-preflight-max-age-seconds",
        str(preflight_max_age_seconds),
        "--output-root",
        str(workflow_root),
    ]
    if docker_handoff_execute:
        workflow_args.append("--docker-handoff-execute")
    with contextlib.redirect_stdout(io.StringIO()):
        workflow_exit_code = workflow_main(workflow_args)
    workflow_summary_path = workflow_root / "renderer_backend_workflow_summary.json"
    workflow_summary = (
        json.loads(workflow_summary_path.read_text(encoding="utf-8"))
        if workflow_summary_path.exists()
        else {"status": "WORKFLOW_SUMMARY_MISSING", "success": False}
    )

    payload = {
        "generated_at_utc": _format_utc(_utc_now()),
        "backend": backend,
        "repo_root": str(repo_root),
        "output_root": str(output_root),
        "summary_path": str(summary_path),
        "seeded_setup_summary_path": str(setup_summary_path),
        "seeded_setup_summary": seeded_setup_summary,
        "workflow_root": str(workflow_root),
        "workflow_exit_code": workflow_exit_code,
        "workflow_summary_path": str(workflow_summary_path),
        "workflow_status": workflow_summary.get("status"),
        "workflow_success": workflow_summary.get("success"),
        "backend_bin_path": str(backend_bin_path),
        "preflight_refresh_requested": True,
        "preflight_max_age_seconds": preflight_max_age_seconds,
        "stale_preflight_offset_seconds": stale_preflight_offset_seconds,
        "workflow": workflow_summary,
        "success": bool(
            workflow_exit_code == 0
            and workflow_summary.get("status") in {"HANDOFF_DOCKER_VERIFIED", "HANDOFF_DOCKER_EXECUTED"}
            and workflow_summary.get("success")
            and workflow_summary.get("docker_handoff", {}).get("requested")
            and workflow_summary.get("docker_handoff", {}).get("executed")
            and workflow_summary.get("docker_handoff", {}).get("return_code") == 0
            and workflow_summary.get("docker_handoff", {}).get("preflight", {}).get("refreshed")
        ),
    }
    _write_json(summary_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    payload = run_renderer_backend_workflow_selftest(
        backend=args.backend,
        repo_root=args.repo_root,
        output_root=args.output_root,
        summary_path=args.summary_path,
        docker_binary=args.docker_binary,
        docker_image=args.docker_image,
        docker_container_workspace=args.docker_container_workspace,
        docker_handoff_execute=args.docker_handoff_execute,
        preflight_max_age_seconds=args.preflight_max_age_seconds,
        stale_preflight_offset_seconds=args.stale_preflight_offset_seconds,
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
