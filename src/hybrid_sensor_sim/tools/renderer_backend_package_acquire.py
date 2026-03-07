from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

from hybrid_sensor_sim.tools.renderer_backend_package_stage import (
    build_renderer_backend_package_stage,
)


_SUPPORTED_BACKENDS = ("awsim", "carla")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve a backend package URL, download it, and optionally stage it into a local runtime."
    )
    parser.add_argument(
        "--backend",
        required=True,
        choices=_SUPPORTED_BACKENDS,
        help="Backend package to acquire.",
    )
    parser.add_argument(
        "--setup-summary",
        type=Path,
        help="renderer_backend_local_setup.json used to resolve acquisition_hints download URLs and preserve HELIOS selections.",
    )
    parser.add_argument(
        "--download-url",
        help="Explicit package URL. If omitted, the first acquisition_hints.<backend>.download_options[*].url is used.",
    )
    parser.add_argument(
        "--download-name",
        help="Override the local download filename. Defaults to the URL basename.",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path.home() / "Downloads",
        help="Directory where the package archive will be downloaded or reused.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Directory where staging artifacts will be written. Defaults to third_party/runtime_backends/<backend>.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_package_acquire.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve URLs and planned paths without downloading or staging anything.",
    )
    parser.add_argument(
        "--overwrite-download",
        action="store_true",
        help="Re-download the archive even if the target path already exists.",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download the archive without invoking the staging tool.",
    )
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON payload at {path} must be an object.")
    return payload


def _resolve_path(raw: str | Path) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _resolve_download_options(setup_summary: dict[str, Any], backend: str) -> list[dict[str, str]]:
    hints = setup_summary.get("acquisition_hints", {})
    backend_hints = hints.get(backend, {}) if isinstance(hints, dict) else {}
    raw_options = backend_hints.get("download_options", [])
    if not isinstance(raw_options, list):
        return []
    options: list[dict[str, str]] = []
    for item in raw_options:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        name = item.get("name")
        if isinstance(url, str) and url.strip():
            options.append(
                {
                    "name": name.strip() if isinstance(name, str) and name.strip() else "",
                    "url": url.strip(),
                }
            )
    return options


def _resolve_local_archive_candidates(setup_summary: dict[str, Any], backend: str) -> list[Path]:
    hints = setup_summary.get("acquisition_hints", {})
    backend_hints = hints.get(backend, {}) if isinstance(hints, dict) else {}
    raw_candidates = backend_hints.get("local_download_candidates", [])
    if not isinstance(raw_candidates, list):
        return []
    candidates: list[Path] = []
    seen: set[str] = set()
    for item in raw_candidates:
        if not isinstance(item, str) or not item.strip():
            continue
        path = _resolve_path(item)
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(path)
    return candidates


def _resolve_download_choice(
    *,
    backend: str,
    setup_summary: dict[str, Any],
    explicit_url: str | None,
    explicit_name: str | None,
) -> tuple[str | None, str | None, str | None, list[str], list[dict[str, str]]]:
    issues: list[str] = []
    options = _resolve_download_options(setup_summary, backend)
    if explicit_url:
        download_url = explicit_url.strip()
        source = "explicit"
        selected_name = explicit_name.strip() if explicit_name else ""
    elif options:
        download_url = options[0]["url"]
        source = "setup_summary"
        selected_name = explicit_name.strip() if explicit_name else options[0]["name"]
    else:
        issues.append(
            f"No download URL resolved for {backend}. Provide --download-url or a setup summary with acquisition_hints.{backend}.download_options."
        )
        return None, None, None, issues, options

    if not selected_name:
        parsed = urlparse(download_url)
        selected_name = Path(parsed.path).name
    if not selected_name:
        issues.append(f"Could not determine a download filename for {backend}.")
        return None, None, source, issues, options
    return download_url, selected_name, source, issues, options


def _select_existing_local_archive(setup_summary: dict[str, Any], backend: str) -> Path | None:
    for candidate in _resolve_local_archive_candidates(setup_summary, backend):
        if candidate.exists():
            return candidate
    return None


def _download_archive(
    *,
    url: str,
    target_path: Path,
    overwrite: bool,
) -> tuple[bool, bool, int, str]:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not overwrite:
        return True, True, target_path.stat().st_size, ""
    temp_path = target_path.with_name(f"{target_path.name}.part")
    if temp_path.exists():
        temp_path.unlink()
    total_bytes = 0
    try:
        with urlopen(url) as response, temp_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                total_bytes += len(chunk)
        shutil.move(str(temp_path), str(target_path))
    except Exception as exc:  # pragma: no cover - defensive path
        if temp_path.exists():
            temp_path.unlink()
        return False, False, total_bytes, str(exc)
    return True, False, total_bytes, ""


def build_renderer_backend_package_acquire(
    *,
    backend: str,
    repo_root: Path,
    setup_summary_path: Path | None = None,
    download_url: str | None = None,
    download_name: str | None = None,
    download_dir: Path | None = None,
    output_root: Path | None = None,
    dry_run: bool = False,
    overwrite_download: bool = False,
    download_only: bool = False,
) -> dict[str, Any]:
    backend = backend.strip().lower()
    if backend not in _SUPPORTED_BACKENDS:
        raise ValueError(f"Unsupported backend: {backend}")
    repo_root = repo_root.resolve()
    download_dir = (
        _resolve_path(download_dir)
        if download_dir is not None
        else (Path.home() / "Downloads").resolve()
    )
    output_root = (
        _resolve_path(output_root)
        if output_root is not None
        else (repo_root / "third_party" / "runtime_backends" / backend).resolve()
    )

    setup_summary: dict[str, Any] = {}
    resolved_setup_summary_path: Path | None = None
    if setup_summary_path is not None:
        resolved_setup_summary_path = _resolve_path(setup_summary_path)
        setup_summary = _load_json(resolved_setup_summary_path)

    local_archive_candidates = _resolve_local_archive_candidates(setup_summary, backend)
    selected_local_archive = None if download_url else _select_existing_local_archive(setup_summary, backend)
    if selected_local_archive is not None:
        resolved_url = None
        resolved_name = selected_local_archive.name
        archive_path = selected_local_archive
        source = "local_candidate"
        issues: list[str] = []
        candidate_options = _resolve_download_options(setup_summary, backend)
    else:
        resolved_url, resolved_name, source, issues, candidate_options = _resolve_download_choice(
            backend=backend,
            setup_summary=setup_summary,
            explicit_url=download_url,
            explicit_name=download_name,
        )
        archive_path = (download_dir / resolved_name).resolve() if resolved_name else None
    download_reused_existing = False
    download_succeeded = False
    download_bytes = 0
    download_message = ""
    stage_summary: dict[str, Any] | None = None
    stage_after_download = not download_only

    if selected_local_archive is not None and archive_path is not None:
        download_succeeded = True
        download_reused_existing = True
        download_bytes = archive_path.stat().st_size
        download_message = "Using existing local archive candidate."
    elif resolved_url is not None and archive_path is not None:
        if dry_run:
            download_succeeded = archive_path.exists()
            download_reused_existing = archive_path.exists()
        else:
            (
                download_succeeded,
                download_reused_existing,
                download_bytes,
                download_message,
            ) = _download_archive(
                url=resolved_url,
                target_path=archive_path,
                overwrite=overwrite_download,
            )
            if not download_succeeded:
                issues.append(download_message or f"Failed to download archive for {backend}.")

    if (
        stage_after_download
        and not dry_run
        and download_succeeded
        and archive_path is not None
    ):
        stage_summary = build_renderer_backend_package_stage(
            backend=backend,
            repo_root=repo_root,
            archive=archive_path,
            setup_summary_path=resolved_setup_summary_path,
            output_root=output_root,
        )
        issues.extend(stage_summary.get("issues", []))

    summary_path = output_root / "renderer_backend_package_acquire.json"
    download_command = (
        f"curl -L --fail -o {shlex.quote(str(archive_path))} {shlex.quote(resolved_url)}"
        if resolved_url is not None and archive_path is not None
        else None
    )
    stage_command = (
        "python3 scripts/stage_renderer_backend_package.py "
        f"--backend {backend} "
        f"--archive {shlex.quote(str(archive_path))}"
        + (
            f" --setup-summary {shlex.quote(str(resolved_setup_summary_path))}"
            if resolved_setup_summary_path is not None
            else ""
        )
        if archive_path is not None
        else None
    )
    readiness = {
        "download_url_resolved": resolved_url is not None or selected_local_archive is not None,
        "download_ready": bool(archive_path and archive_path.exists()),
        "download_performed": bool(not dry_run and download_succeeded and not download_reused_existing),
        "download_reused_existing": download_reused_existing,
        "stage_requested": stage_after_download,
        "stage_ready": bool(
            stage_summary
            and stage_summary.get("readiness", {}).get("backend_executable_ready", False)
        ),
    }
    return {
        "backend": backend,
        "setup_summary_path": str(resolved_setup_summary_path) if resolved_setup_summary_path else None,
        "download": {
            "url": resolved_url,
            "source": source,
            "name": resolved_name,
            "target_path": str(archive_path) if archive_path is not None else None,
            "target_exists": bool(archive_path and archive_path.exists()),
            "download_dir": str(download_dir),
            "bytes_downloaded": download_bytes,
            "message": download_message,
            "candidate_options": candidate_options,
            "local_archive_candidates": [str(path) for path in local_archive_candidates],
            "used_local_archive": str(selected_local_archive) if selected_local_archive is not None else None,
        },
        "readiness": readiness,
        "dry_run": dry_run,
        "download_only": download_only,
        "overwrite_download": overwrite_download,
        "issues": issues,
        "commands": {
            "download": download_command,
            "stage": stage_command,
            "download_and_stage": (
                f"{download_command} && {stage_command}"
                if download_command and stage_command
                else None
            ),
            "smoke": stage_summary.get("commands", {}).get("smoke") if stage_summary else None,
        },
        "stage": stage_summary,
        "artifacts": {
            "summary_path": str(summary_path),
            "stage_summary_path": (
                stage_summary.get("artifacts", {}).get("summary_path")
                if stage_summary is not None
                else None
            ),
            "stage_env_path": (
                stage_summary.get("artifacts", {}).get("env_path")
                if stage_summary is not None
                else None
            ),
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    summary = build_renderer_backend_package_acquire(
        backend=args.backend,
        repo_root=repo_root,
        setup_summary_path=_resolve_path(args.setup_summary) if args.setup_summary is not None else None,
        download_url=args.download_url,
        download_name=args.download_name,
        download_dir=_resolve_path(args.download_dir),
        output_root=_resolve_path(args.output_root) if args.output_root is not None else None,
        dry_run=args.dry_run,
        overwrite_download=args.overwrite_download,
        download_only=args.download_only,
    )
    summary_path = (
        _resolve_path(args.summary_path)
        if args.summary_path is not None
        else Path(summary["artifacts"]["summary_path"])
    )
    summary["artifacts"]["summary_path"] = str(summary_path)
    _write_json(summary_path, summary)
    print(json.dumps(summary, indent=2))

    success = summary["readiness"]["download_url_resolved"]
    if not args.dry_run:
        success = success and summary["readiness"]["download_ready"]
        if not args.download_only:
            success = success and summary["readiness"]["stage_ready"]
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
