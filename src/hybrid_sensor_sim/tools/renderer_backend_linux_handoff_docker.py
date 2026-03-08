from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.renderer_backend_workflow import _resolve_path

_DEFAULT_DOCKER_IMAGE = "python:3.11-slim"
_DEFAULT_CONTAINER_WORKSPACE = "/workspace"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run renderer backend Linux handoff verification/execution inside a Docker Linux container."
    )
    parser.add_argument("--bundle", type=Path, required=True, help="Path to the handoff bundle tar.gz.")
    parser.add_argument(
        "--transfer-manifest",
        type=Path,
        required=True,
        help="Path to renderer_backend_workflow_linux_handoff_transfer_manifest.json.",
    )
    parser.add_argument(
        "--bundle-manifest",
        type=Path,
        help="Optional path to renderer_backend_workflow_linux_handoff_bundle_manifest.json.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="Repo checkout to mount as the container workspace. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/renderer_backend_linux_handoff_docker"),
        help="Directory where the Docker run summary will be written.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_linux_handoff_docker_run.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker CLI binary to invoke.",
    )
    parser.add_argument(
        "--docker-image",
        default=_DEFAULT_DOCKER_IMAGE,
        help="Linux Docker image that will execute run_renderer_backend_linux_handoff.py.",
    )
    parser.add_argument(
        "--container-workspace",
        default=_DEFAULT_CONTAINER_WORKSPACE,
        help="Workspace mount point inside the container.",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Verify only; do not execute the extracted handoff script inside the container.",
    )
    parser.add_argument(
        "--forward-arg",
        action="append",
        default=[],
        help="Forwarded to the handoff helper inside the container.",
    )
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _containerize_path(
    *,
    host_path: Path,
    repo_root: Path,
    container_workspace: str,
    mount_table: dict[str, dict[str, Any]],
    counter: list[int],
) -> str:
    host_path = host_path.resolve()
    try:
        relative = host_path.relative_to(repo_root)
    except ValueError:
        mount_root = host_path.parent if host_path.is_file() else host_path
        mount_key = str(mount_root)
        mount_info = mount_table.get(mount_key)
        if mount_info is None:
            container_root = f"/handoff_input_{counter[0]}"
            counter[0] += 1
            mount_info = {
                "host_path": str(mount_root),
                "container_path": container_root,
                "read_only": True,
            }
            mount_table[mount_key] = mount_info
        if host_path.is_dir():
            return mount_info["container_path"]
        return f"{mount_info['container_path']}/{host_path.name}"
    if host_path.is_dir():
        return f"{container_workspace}/{relative}".rstrip("/")
    return f"{container_workspace}/{relative}"


def run_renderer_backend_linux_handoff_in_docker(
    *,
    bundle_path: Path,
    transfer_manifest_path: Path,
    bundle_manifest_path: Path | None = None,
    repo_root: Path | None = None,
    output_root: Path,
    summary_path: Path | None = None,
    docker_binary: str = "docker",
    docker_image: str = _DEFAULT_DOCKER_IMAGE,
    container_workspace: str = _DEFAULT_CONTAINER_WORKSPACE,
    skip_run: bool = False,
    forward_args: list[str] | None = None,
) -> dict[str, Any]:
    repo_root = _resolve_path(repo_root) if repo_root is not None else Path.cwd().resolve()
    output_root = _ensure_directory(_resolve_path(output_root))
    summary_path = (
        _resolve_path(summary_path)
        if summary_path is not None
        else (output_root / "renderer_backend_linux_handoff_docker_run.json").resolve()
    )
    bundle_path = _resolve_path(bundle_path)
    transfer_manifest_path = _resolve_path(transfer_manifest_path)
    bundle_manifest_path = (
        _resolve_path(bundle_manifest_path)
        if bundle_manifest_path is not None
        else None
    )
    forward_args = list(forward_args or [])

    mount_table: dict[str, dict[str, Any]] = {
        str(repo_root): {
            "host_path": str(repo_root),
            "container_path": container_workspace,
            "read_only": False,
        }
    }
    counter = [0]
    bundle_container_path = _containerize_path(
        host_path=bundle_path,
        repo_root=repo_root,
        container_workspace=container_workspace,
        mount_table=mount_table,
        counter=counter,
    )
    transfer_manifest_container_path = _containerize_path(
        host_path=transfer_manifest_path,
        repo_root=repo_root,
        container_workspace=container_workspace,
        mount_table=mount_table,
        counter=counter,
    )
    bundle_manifest_container_path = (
        _containerize_path(
            host_path=bundle_manifest_path,
            repo_root=repo_root,
            container_workspace=container_workspace,
            mount_table=mount_table,
            counter=counter,
        )
        if bundle_manifest_path is not None
        else None
    )

    try:
        output_relative = output_root.relative_to(repo_root)
        output_container_path = f"{container_workspace}/{output_relative}"
    except ValueError:
        mount_key = str(output_root)
        mount_table[mount_key] = {
            "host_path": str(output_root),
            "container_path": "/handoff_output",
            "read_only": False,
        }
        output_container_path = "/handoff_output"

    command = [
        docker_binary,
        "run",
        "--rm",
    ]
    mounts = []
    for mount in mount_table.values():
        mount_arg = f"{mount['host_path']}:{mount['container_path']}"
        if mount["read_only"]:
            mount_arg += ":ro"
        mounts.append(mount_arg)
        command.extend(["-v", mount_arg])
    command.extend(
        [
            "-w",
            container_workspace,
            docker_image,
            "python3",
            f"{container_workspace}/scripts/run_renderer_backend_linux_handoff.py",
            "--bundle",
            bundle_container_path,
            "--transfer-manifest",
            transfer_manifest_container_path,
            "--repo-root",
            container_workspace,
            "--output-root",
            output_container_path,
        ]
    )
    if bundle_manifest_container_path is not None:
        command.extend(["--bundle-manifest", bundle_manifest_container_path])
    if skip_run:
        command.append("--skip-run")
    for item in forward_args:
        command.extend(["--forward-arg", item])

    proc = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
        env=os.environ.copy(),
    )
    summary = {
        "bundle_path": str(bundle_path),
        "transfer_manifest_path": str(transfer_manifest_path),
        "bundle_manifest_path": str(bundle_manifest_path) if bundle_manifest_path is not None else None,
        "repo_root": str(repo_root),
        "output_root": str(output_root),
        "summary_path": str(summary_path),
        "docker_binary": docker_binary,
        "docker_image": docker_image,
        "container_workspace": container_workspace,
        "mounts": mounts,
        "container_paths": {
            "bundle": bundle_container_path,
            "transfer_manifest": transfer_manifest_container_path,
            "bundle_manifest": bundle_manifest_container_path,
            "output_root": output_container_path,
        },
        "command": command,
        "skip_run": skip_run,
        "forward_args": forward_args,
        "return_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }
    _write_json(summary_path, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    summary = run_renderer_backend_linux_handoff_in_docker(
        bundle_path=args.bundle,
        transfer_manifest_path=args.transfer_manifest,
        bundle_manifest_path=args.bundle_manifest,
        repo_root=args.repo_root,
        output_root=args.output_root,
        summary_path=args.summary_path,
        docker_binary=args.docker_binary,
        docker_image=args.docker_image,
        container_workspace=args.container_workspace,
        skip_run=args.skip_run,
        forward_args=list(args.forward_arg),
    )
    print(json.dumps(summary, indent=2))
    return int(summary["return_code"])


if __name__ == "__main__":
    raise SystemExit(main())
