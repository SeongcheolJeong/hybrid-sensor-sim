from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import tarfile
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.renderer_backend_linux_handoff_docker import (
    run_renderer_backend_linux_handoff_in_docker,
)

_DEFAULT_DOCKER_IMAGE = "python:3.11-slim"
_DEFAULT_CONTAINER_WORKSPACE = "/workspace"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a synthetic Linux handoff bundle and run it through the Docker handoff helper."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="Repo root to mount into Docker. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/renderer_backend_linux_handoff_selftest"),
        help="Directory where self-test artifacts will be written.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_linux_handoff_selftest.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker CLI binary to invoke.",
    )
    parser.add_argument(
        "--docker-image",
        default=_DEFAULT_DOCKER_IMAGE,
        help="Linux Docker image used for the handoff helper.",
    )
    parser.add_argument(
        "--container-workspace",
        default=_DEFAULT_CONTAINER_WORKSPACE,
        help="Workspace mount point inside the container.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the extracted handoff script inside the container. Default is verify-only.",
    )
    parser.add_argument(
        "--marker-relative-path",
        default="artifacts/renderer_backend_linux_handoff_selftest/selftest_marker.txt",
        help="Repo-relative marker path written by the synthetic handoff script during execute mode.",
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


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_selftest_bundle(
    *,
    repo_root: Path,
    output_root: Path,
    marker_relative_path: str,
) -> dict[str, str]:
    bundle_root = output_root / "bundle_src"
    handoff_root = bundle_root / "artifacts" / "renderer_backend_workflow" / "selftest"
    handoff_script_path = handoff_root / "renderer_backend_workflow_linux_handoff.sh"
    handoff_config_path = handoff_root / "renderer_backend_workflow_linux_handoff_config.json"
    handoff_env_path = handoff_root / "renderer_backend_workflow_linux_handoff.env.sh"
    marker_path = repo_root / marker_relative_path

    _write_text(
        handoff_script_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'MARKER_PATH="$WORKFLOW_REPO_ROOT/{marker_relative_path}"',
                'mkdir -p "$(dirname "$MARKER_PATH")"',
                'printf \'selftest-ok\\n\' > "$MARKER_PATH"',
                "",
            ]
        ),
    )
    handoff_script_path.chmod(0o755)
    _write_text(handoff_config_path, "{}\n")
    _write_text(handoff_env_path, "#!/usr/bin/env bash\n")
    handoff_env_path.chmod(0o755)

    bundle_path = output_root / "renderer_backend_linux_handoff_selftest_bundle.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as handle:
        for path in sorted(bundle_root.rglob("*")):
            handle.add(path, arcname=str(path.relative_to(bundle_root)))

    transfer_manifest_path = output_root / "renderer_backend_linux_handoff_selftest_transfer_manifest.json"
    bundle_manifest_path = output_root / "renderer_backend_linux_handoff_selftest_bundle_manifest.json"

    entries: list[dict[str, Any]] = []
    verifiable_entries: list[dict[str, Any]] = []
    for kind, path in [
        ("handoff_generated_script", handoff_script_path),
        ("handoff_generated_config", handoff_config_path),
        ("handoff_generated_env", handoff_env_path),
    ]:
        target_relative_path = str(path.relative_to(bundle_root))
        entry = {
            "kind": kind,
            "local_path": str(path),
            "target_relative_path": target_relative_path,
            "sha256": _sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        entries.append(entry)
        verifiable_entries.append(
            {
                "kind": kind,
                "target_relative_path": target_relative_path,
                "sha256": entry["sha256"],
                "size_bytes": entry["size_bytes"],
            }
        )
    _write_json(
        transfer_manifest_path,
        {
            "entries": entries,
            "verifiable_entries": verifiable_entries,
        },
    )
    _write_json(
        bundle_manifest_path,
        {
            "bundle_path": str(bundle_path),
            "bundle_sha256": _sha256_file(bundle_path),
            "transfer_manifest_path": str(transfer_manifest_path),
            "entry_count": len(entries),
        },
    )
    return {
        "bundle_path": str(bundle_path),
        "transfer_manifest_path": str(transfer_manifest_path),
        "bundle_manifest_path": str(bundle_manifest_path),
        "marker_path": str(marker_path),
        "handoff_script_path": str(handoff_script_path),
    }


def run_renderer_backend_linux_handoff_selftest(
    *,
    repo_root: Path | None = None,
    output_root: Path,
    summary_path: Path | None = None,
    docker_binary: str = "docker",
    docker_image: str = _DEFAULT_DOCKER_IMAGE,
    container_workspace: str = _DEFAULT_CONTAINER_WORKSPACE,
    execute: bool = False,
    marker_relative_path: str = "artifacts/renderer_backend_linux_handoff_selftest/selftest_marker.txt",
) -> dict[str, Any]:
    repo_root = _resolve_path(repo_root) if repo_root is not None else Path.cwd().resolve()
    output_root = _resolve_path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = (
        _resolve_path(summary_path)
        if summary_path is not None
        else (output_root / "renderer_backend_linux_handoff_selftest.json").resolve()
    )

    bundle_artifacts = _build_selftest_bundle(
        repo_root=repo_root,
        output_root=output_root,
        marker_relative_path=marker_relative_path,
    )
    marker_path = Path(bundle_artifacts["marker_path"])
    if marker_path.exists():
        marker_path.unlink()

    docker_summary = run_renderer_backend_linux_handoff_in_docker(
        bundle_path=Path(bundle_artifacts["bundle_path"]),
        transfer_manifest_path=Path(bundle_artifacts["transfer_manifest_path"]),
        bundle_manifest_path=Path(bundle_artifacts["bundle_manifest_path"]),
        repo_root=repo_root,
        output_root=output_root / "docker_run",
        docker_binary=docker_binary,
        docker_image=docker_image,
        container_workspace=container_workspace,
        skip_run=not execute,
    )

    marker_exists = marker_path.exists()
    marker_content = marker_path.read_text(encoding="utf-8").strip() if marker_exists else None
    summary = {
        "generated_at_utc": _format_utc(_utc_now()),
        "repo_root": str(repo_root),
        "output_root": str(output_root),
        "summary_path": str(summary_path),
        "execute": execute,
        "marker_relative_path": marker_relative_path,
        "marker_path": str(marker_path),
        "marker_exists": marker_exists,
        "marker_content": marker_content,
        "bundle_artifacts": bundle_artifacts,
        "docker": docker_summary,
        "success": docker_summary.get("return_code") == 0 and (marker_exists if execute else True),
    }
    _write_json(summary_path, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    summary = run_renderer_backend_linux_handoff_selftest(
        repo_root=args.repo_root,
        output_root=args.output_root,
        summary_path=args.summary_path,
        docker_binary=args.docker_binary,
        docker_image=args.docker_image,
        container_workspace=args.container_workspace,
        execute=args.execute,
        marker_relative_path=args.marker_relative_path,
    )
    print(json.dumps(summary, indent=2))
    return 0 if summary["success"] else int(summary["docker"].get("return_code") or 1)


if __name__ == "__main__":
    raise SystemExit(main())
