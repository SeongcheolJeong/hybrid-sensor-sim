from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import stat
import tarfile
import zipfile
from pathlib import Path
from typing import Any


_DEFAULT_BACKEND_MAPS = {
    "awsim": "SampleMap",
    "carla": "Town03",
}

_BACKEND_STAGE_CONFIG: dict[str, dict[str, Any]] = {
    "awsim": {
        "env_var": "AWSIM_BIN",
        "map_env_var": "AWSIM_RENDERER_MAP",
        "default_map": _DEFAULT_BACKEND_MAPS["awsim"],
        "executable_names": [
            "AWSIM-Demo.x86_64",
            "AWSIM-Demo-Lightweight.x86_64",
            "AWSIM.x86_64",
            "AWSIM",
        ],
        "smoke_binary_config": "configs/renderer_backend_smoke.awsim.local.example.json",
        "smoke_docker_config": "configs/renderer_backend_smoke.awsim.local.docker.example.json",
    },
    "carla": {
        "env_var": "CARLA_BIN",
        "map_env_var": "CARLA_RENDERER_MAP",
        "default_map": _DEFAULT_BACKEND_MAPS["carla"],
        "executable_names": [
            "CarlaUnreal.sh",
            "CarlaUE5.sh",
            "CarlaUE4.sh",
            "CarlaUnreal.exe",
            "CarlaUE5",
            "CarlaUE4",
        ],
        "smoke_binary_config": "configs/renderer_backend_smoke.carla.local.example.json",
        "smoke_docker_config": "configs/renderer_backend_smoke.carla.local.docker.example.json",
    },
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage a packaged AWSIM/CARLA archive into a reusable local runtime directory."
    )
    parser.add_argument(
        "--backend",
        required=True,
        choices=sorted(_BACKEND_STAGE_CONFIG.keys()),
        help="Backend runtime to stage from a packaged archive.",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        help="Explicit runtime archive (.zip/.tar.gz/.tgz). If omitted, the first local_download_candidate from --setup-summary is used.",
    )
    parser.add_argument(
        "--setup-summary",
        type=Path,
        help="renderer_backend_local_setup.json used to auto-pick a local download candidate and preserve existing HELIOS selections.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Directory where the archive will be extracted and staged. Defaults to third_party/runtime_backends/<backend>.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_package_stage.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--env-path",
        type=Path,
        help="Where to write renderer_backend_package_stage.env.sh. Defaults under output-root.",
    )
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


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


def _archive_candidates_from_setup(setup_summary: dict[str, Any], backend: str) -> list[Path]:
    hints = setup_summary.get("acquisition_hints", {})
    backend_hints = hints.get(backend, {}) if isinstance(hints, dict) else {}
    raw_candidates = backend_hints.get("local_download_candidates", [])
    if not isinstance(raw_candidates, list):
        return []
    candidates: list[Path] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        if not isinstance(raw, str) or not raw.strip():
            continue
        path = _resolve_path(raw)
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(path)
    return candidates


def _select_archive(
    *,
    backend: str,
    archive: Path | None,
    setup_summary: dict[str, Any],
) -> tuple[Path | None, str | None, list[str]]:
    issues: list[str] = []
    if archive is not None:
        resolved = _resolve_path(archive)
        if not resolved.exists():
            issues.append(f"Archive does not exist: {resolved}")
            return None, "explicit", issues
        return resolved, "explicit", issues

    candidates = _archive_candidates_from_setup(setup_summary, backend)
    for candidate in candidates:
        if candidate.exists():
            return candidate, "setup_summary", issues
    if candidates:
        issues.append(f"No existing archive found among setup summary candidates for {backend}.")
    else:
        issues.append(
            f"No archive candidate available for {backend}. Provide --archive or a setup summary with local_download_candidates."
        )
    return None, None, issues


def _extract_archive(archive_path: Path, extract_dir: Path) -> tuple[bool, str]:
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    name = archive_path.name.lower()
    try:
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as handle:
                handle.extractall(extract_dir)
        elif name.endswith(".tar.gz") or name.endswith(".tgz"):
            with tarfile.open(archive_path, "r:*") as handle:
                handle.extractall(extract_dir)
        else:
            return False, f"Unsupported archive format: {archive_path.name}"
    except (OSError, zipfile.BadZipFile, tarfile.TarError) as exc:
        return False, str(exc)
    return True, ""


def _find_backend_executable(backend: str, extract_dir: Path) -> tuple[Path | None, list[str]]:
    config = _BACKEND_STAGE_CONFIG[backend]
    preferred_names = {name.lower(): index for index, name in enumerate(config["executable_names"])}
    candidates: list[tuple[int, int, str, Path]] = []
    scanned: list[str] = []
    for path in extract_dir.rglob("*"):
        if not path.is_file():
            continue
        name_lower = path.name.lower()
        if name_lower not in preferred_names:
            continue
        scanned.append(str(path.resolve()))
        depth = len(path.relative_to(extract_dir).parts)
        candidates.append((preferred_names[name_lower], depth, str(path.resolve()), path.resolve()))
    candidates.sort()
    if not candidates:
        return None, scanned
    return candidates[0][3], scanned


def _ensure_executable(path: Path) -> bool:
    if path.suffix.lower() == ".exe":
        return False
    mode = path.stat().st_mode
    updated_mode = mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    if updated_mode == mode:
        return False
    path.chmod(updated_mode)
    return True


def _merge_selection(
    *,
    backend: str,
    executable_path: Path | None,
    setup_summary: dict[str, Any],
) -> dict[str, str | None]:
    config = _BACKEND_STAGE_CONFIG[backend]
    selection = {}
    raw_selection = setup_summary.get("selection", {})
    if isinstance(raw_selection, dict):
        for key, value in raw_selection.items():
            selection[key] = str(value) if value is not None else None

    for env_key in ("HELIOS_BIN", "HELIOS_DOCKER_IMAGE", "HELIOS_DOCKER_BINARY"):
        if not selection.get(env_key):
            env_value = os.getenv(env_key, "").strip()
            if env_value:
                selection[env_key] = env_value

    selection[config["env_var"]] = str(executable_path) if executable_path is not None else None
    map_env_var = config["map_env_var"]
    map_value = selection.get(map_env_var) or os.getenv(map_env_var, "").strip() or config["default_map"]
    selection[map_env_var] = map_value
    return selection


def _render_env_file(summary: dict[str, Any]) -> str:
    selection = summary.get("selection", {})
    readiness = summary.get("readiness", {})
    commands = summary.get("commands", {})
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by renderer_backend_package_stage.py",
        "# Source this file before running backend smoke presets.",
        "",
    ]
    for env_key in (
        "HELIOS_BIN",
        "HELIOS_DOCKER_IMAGE",
        "HELIOS_DOCKER_BINARY",
        "AWSIM_BIN",
        "AWSIM_RENDERER_MAP",
        "CARLA_BIN",
        "CARLA_RENDERER_MAP",
    ):
        value = selection.get(env_key)
        if value:
            lines.append(f"export {env_key}={shlex.quote(str(value))}")
        else:
            lines.append(f"# export {env_key}=<set-me>")
    lines.extend(
        [
            "",
            f"# backend={summary.get('backend')}",
            f"# archive_resolved={readiness.get('archive_resolved', False)}",
            f"# backend_executable_ready={readiness.get('backend_executable_ready', False)}",
            f"# smoke_ready_binary={readiness.get('smoke_ready_binary', False)}",
            f"# smoke_ready_docker={readiness.get('smoke_ready_docker', False)}",
            "#",
        ]
    )
    if commands.get("smoke"):
        lines.append(f"# smoke: {commands['smoke']}")
    if commands.get("local_discovery_refresh"):
        lines.append(f"# refresh discovery: {commands['local_discovery_refresh']}")
    lines.append("")
    return "\n".join(lines)


def build_renderer_backend_package_stage(
    *,
    backend: str,
    repo_root: Path,
    archive: Path | None = None,
    setup_summary_path: Path | None = None,
    output_root: Path | None = None,
) -> dict[str, Any]:
    backend = backend.strip().lower()
    if backend not in _BACKEND_STAGE_CONFIG:
        raise ValueError(f"Unsupported backend: {backend}")
    repo_root = repo_root.resolve()
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

    archive_path, archive_source, issues = _select_archive(
        backend=backend,
        archive=archive,
        setup_summary=setup_summary,
    )
    extracted_dir = output_root / "expanded"
    executable_path: Path | None = None
    chmod_updated = False
    extraction_message = ""
    extraction_succeeded = False
    executable_candidates: list[str] = []
    if archive_path is not None:
        extraction_succeeded, extraction_message = _extract_archive(archive_path, extracted_dir)
        if extraction_succeeded:
            executable_path, executable_candidates = _find_backend_executable(backend, extracted_dir)
            if executable_path is not None:
                chmod_updated = _ensure_executable(executable_path)
            else:
                issues.append(
                    f"Could not locate a supported {backend} executable under {extracted_dir}."
                )
        else:
            issues.append(extraction_message)
    else:
        extraction_message = "Archive was not resolved."

    selection = _merge_selection(
        backend=backend,
        executable_path=executable_path,
        setup_summary=setup_summary,
    )
    config = _BACKEND_STAGE_CONFIG[backend]
    env_var = config["env_var"]
    readiness = {
        "archive_resolved": archive_path is not None,
        "archive_extracted": extraction_succeeded,
        "backend_executable_ready": executable_path is not None and os.access(executable_path, os.X_OK),
        "helios_binary_ready": bool(selection.get("HELIOS_BIN")),
        "helios_docker_ready": bool(
            selection.get("HELIOS_DOCKER_IMAGE") and selection.get("HELIOS_DOCKER_BINARY")
        ),
    }
    readiness["smoke_ready_binary"] = (
        readiness["backend_executable_ready"] and readiness["helios_binary_ready"]
    )
    readiness["smoke_ready_docker"] = (
        readiness["backend_executable_ready"] and readiness["helios_docker_ready"]
    )
    readiness["smoke_ready"] = readiness["smoke_ready_binary"] or readiness["smoke_ready_docker"]

    env_path = output_root / "renderer_backend_package_stage.env.sh"
    summary_path = output_root / "renderer_backend_package_stage.json"
    smoke_binary_command = (
        f"source {shlex.quote(str(env_path))} && "
        "python3 scripts/run_renderer_backend_smoke.py "
        f"--config {config['smoke_binary_config']} --backend {backend}"
    )
    smoke_docker_command = (
        f"source {shlex.quote(str(env_path))} && "
        "python3 scripts/run_renderer_backend_smoke.py "
        f"--config {config['smoke_docker_config']} --backend {backend}"
    )
    smoke_command = (
        smoke_binary_command
        if readiness["smoke_ready_binary"]
        else smoke_docker_command
    )
    return {
        "backend": backend,
        "archive_path": str(archive_path) if archive_path is not None else None,
        "archive_source": archive_source,
        "setup_summary_path": str(resolved_setup_summary_path) if resolved_setup_summary_path else None,
        "archive_candidates": [
            str(path) for path in _archive_candidates_from_setup(setup_summary, backend)
        ],
        "selection": selection,
        "readiness": readiness,
        "issues": issues,
        "staging": {
            "output_root": str(output_root),
            "extracted_dir": str(extracted_dir),
            "extraction_message": extraction_message,
            "chmod_updated": chmod_updated,
            "selected_executable_path": str(executable_path) if executable_path is not None else None,
            "selected_executable_name": executable_path.name if executable_path is not None else None,
            "selected_env_var": env_var,
            "executable_candidates": executable_candidates,
        },
        "commands": {
            "local_discovery_refresh": "python3 scripts/discover_renderer_backend_local_env.py",
            "smoke_binary": smoke_binary_command,
            "smoke_docker": smoke_docker_command,
            "smoke": smoke_command,
        },
        "artifacts": {
            "summary_path": str(summary_path),
            "env_path": str(env_path),
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    summary = build_renderer_backend_package_stage(
        backend=args.backend,
        repo_root=repo_root,
        archive=_resolve_path(args.archive) if args.archive is not None else None,
        setup_summary_path=_resolve_path(args.setup_summary) if args.setup_summary is not None else None,
        output_root=_resolve_path(args.output_root) if args.output_root is not None else None,
    )
    summary_path = (
        _resolve_path(args.summary_path)
        if args.summary_path is not None
        else Path(summary["artifacts"]["summary_path"])
    )
    env_path = (
        _resolve_path(args.env_path)
        if args.env_path is not None
        else Path(summary["artifacts"]["env_path"])
    )
    summary["artifacts"]["summary_path"] = str(summary_path)
    summary["artifacts"]["env_path"] = str(env_path)
    _write_json(summary_path, summary)
    _write_text(env_path, _render_env_file(summary))
    print(json.dumps(summary, indent=2))
    return 0 if summary["readiness"]["backend_executable_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
