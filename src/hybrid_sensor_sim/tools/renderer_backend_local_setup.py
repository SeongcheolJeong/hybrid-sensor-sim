from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import subprocess
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest


_DEFAULT_BACKEND_MAPS = {
    "awsim": "SampleMap",
    "carla": "Town03",
}

_DEFAULT_HELIOS_DOCKER_IMAGE = "heliosplusplus:cli"
_DEFAULT_HELIOS_DOCKER_BINARY = "/home/jovyan/helios/build/helios++"
_DEFAULT_HELIOS_DOCKER_MOUNT_POINT = "/workspace"
_AWSIM_EXECUTABLE_NAMES = {
    "awsim",
    "awsim.x86_64",
    "awsim-demo.x86_64",
    "awsim-demo-lightweight.x86_64",
}
_CARLA_EXECUTABLE_NAMES = {
    "carlaue4.sh",
    "carlaue4",
    "carlaue5.sh",
    "carlaue5",
    "carlaunreal.sh",
    "carlaunreal.exe",
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover local HELIOS/AWSIM/CARLA runtime candidates and emit env/setup artifacts."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/renderer_backend_local_setup"),
        help="Directory where discovery artifacts will be written.",
    )
    parser.add_argument(
        "--search-root",
        action="append",
        default=[],
        help="Additional root to scan for local backend binaries or source checkouts.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_local_setup.json. Defaults under output_dir.",
    )
    parser.add_argument(
        "--env-path",
        type=Path,
        help="Where to write renderer_backend_local.env.sh. Defaults under output_dir.",
    )
    parser.add_argument(
        "--no-default-search-roots",
        action="store_true",
        help="Only scan explicit --search-root paths plus the repo root.",
    )
    parser.add_argument(
        "--probe-helios-docker-demo",
        action="store_true",
        help="Run the HELIOS docker demo config and store the result summary.",
    )
    parser.add_argument(
        "--helios-docker-probe-config",
        type=Path,
        default=Path("configs/hybrid_sensor_sim.helios_docker.json"),
        help="Config to use when --probe-helios-docker-demo is enabled.",
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
        raise ValueError(f"Config payload at {path} must be a JSON object.")
    return payload


def _resolve_runtime_path(raw: str | Path) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _is_executable_file(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def _rosetta_available() -> bool:
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        return False
    try:
        proc = subprocess.run(
            ["pkgutil", "--pkg-info", "com.apple.pkg.RosettaUpdateAuto"],
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return False
    return proc.returncode == 0


def _extract_architectures_from_file_description(description: str) -> list[str]:
    lowered = description.lower()
    architectures: list[str] = []
    for token, normalized in (
        ("arm64", "arm64"),
        ("aarch64", "arm64"),
        ("x86_64", "x86_64"),
        ("x86-64", "x86_64"),
        ("i386", "x86"),
        ("80386", "x86"),
    ):
        if token in lowered and normalized not in architectures:
            architectures.append(normalized)
    return architectures


def _evaluate_binary_compatibility(
    *,
    system: str,
    machine: str,
    binary_format: str,
    architectures: list[str],
) -> tuple[bool, str, str | None]:
    if binary_format in {"script", "unknown"}:
        return True, "", None
    if binary_format == "mach-o":
        if system != "Darwin":
            return False, f"Mach-O binary is not supported on {system}", None
        if not architectures:
            return True, "", None
        if machine in architectures:
            return True, "", None
        if machine == "arm64" and "x86_64" in architectures:
            if _rosetta_available():
                return True, "", "rosetta"
            return False, "Mach-O x86_64 binary requires Rosetta on Darwin arm64", "rosetta"
        return False, f"Mach-O architecture {architectures} is not supported on Darwin {machine}", None
    if binary_format == "elf":
        if system != "Linux":
            return False, f"ELF binary is not supported on {system}", None
        if not architectures or machine in architectures:
            return True, "", None
        return False, f"ELF architecture {architectures} is not supported on Linux {machine}", None
    if binary_format == "pe":
        if system != "Windows":
            return False, f"Windows executable is not supported on {system}", None
        if not architectures or machine in architectures:
            return True, "", None
        return False, f"Windows executable architecture {architectures} is not supported on Windows {machine}", None
    return True, "", None


def _inspect_executable_host_compatibility(path: Path) -> dict[str, Any]:
    host = _host_platform_summary()
    system = host["system"]
    machine = host["machine"]
    path = path.resolve()
    if not path.exists():
        return {
            "host_compatible": False,
            "host_compatibility_reason": "file does not exist",
            "binary_format": "missing",
            "file_description": "",
            "binary_architectures": [],
            "translation_required": None,
        }
    if not _is_executable_file(path):
        return {
            "host_compatible": False,
            "host_compatibility_reason": "file is not executable",
            "binary_format": "non_executable",
            "file_description": "",
            "binary_architectures": [],
            "translation_required": None,
        }

    description = ""
    try:
        proc = subprocess.run(
            ["file", "-b", str(path)],
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode == 0:
            description = proc.stdout.strip()
    except FileNotFoundError:
        description = ""

    lowered = description.lower()
    if not lowered:
        try:
            with path.open("rb") as handle:
                prefix = handle.read(4)
        except OSError:
            prefix = b""
        if prefix.startswith(b"#!"):
            return {
                "host_compatible": True,
                "host_compatibility_reason": "",
                "binary_format": "script",
                "file_description": "script (shebang detected)",
                "binary_architectures": [],
                "translation_required": None,
            }
        if path.suffix.lower() == ".exe":
            architectures = ["x86_64"]
            host_compatible, reason, translation_required = _evaluate_binary_compatibility(
                system=system,
                machine=machine,
                binary_format="pe",
                architectures=architectures,
            )
            return {
                "host_compatible": host_compatible,
                "host_compatibility_reason": reason,
                "binary_format": "pe",
                "file_description": "",
                "binary_architectures": architectures,
                "translation_required": translation_required,
            }
        return {
            "host_compatible": True,
            "host_compatibility_reason": "",
            "binary_format": "unknown",
            "file_description": "",
            "binary_architectures": [],
            "translation_required": None,
        }

    architectures = _extract_architectures_from_file_description(description)
    if "script" in lowered or "text executable" in lowered:
        return {
            "host_compatible": True,
            "host_compatibility_reason": "",
            "binary_format": "script",
            "file_description": description,
            "binary_architectures": architectures,
            "translation_required": None,
        }
    if "mach-o" in lowered:
        host_compatible, reason, translation_required = _evaluate_binary_compatibility(
            system=system,
            machine=machine,
            binary_format="mach-o",
            architectures=architectures,
        )
        return {
            "host_compatible": host_compatible,
            "host_compatibility_reason": reason,
            "binary_format": "mach-o",
            "file_description": description,
            "binary_architectures": architectures,
            "translation_required": translation_required,
        }
    if "elf" in lowered:
        host_compatible, reason, translation_required = _evaluate_binary_compatibility(
            system=system,
            machine=machine,
            binary_format="elf",
            architectures=architectures,
        )
        return {
            "host_compatible": host_compatible,
            "host_compatibility_reason": reason,
            "binary_format": "elf",
            "file_description": description,
            "binary_architectures": architectures,
            "translation_required": translation_required,
        }
    if "pe32" in lowered or "ms-dos executable" in lowered or "windows" in lowered:
        host_compatible, reason, translation_required = _evaluate_binary_compatibility(
            system=system,
            machine=machine,
            binary_format="pe",
            architectures=architectures,
        )
        return {
            "host_compatible": host_compatible,
            "host_compatibility_reason": reason,
            "binary_format": "pe",
            "file_description": description,
            "binary_architectures": architectures,
            "translation_required": translation_required,
        }
    return {
        "host_compatible": True,
        "host_compatibility_reason": "",
        "binary_format": "unknown",
        "file_description": description,
        "binary_architectures": architectures,
        "translation_required": None,
    }


def _path_record(path: Path, *, origin: str, kind: str) -> dict[str, Any]:
    compatibility = _inspect_executable_host_compatibility(path)
    return {
        "path": str(path.resolve()),
        "origin": origin,
        "kind": kind,
        "exists": path.exists(),
        "executable": _is_executable_file(path),
        "host_compatible": compatibility.get("host_compatible", False),
        "host_compatibility_reason": compatibility.get("host_compatibility_reason", ""),
        "binary_format": compatibility.get("binary_format", "unknown"),
        "file_description": compatibility.get("file_description", ""),
        "binary_architectures": compatibility.get("binary_architectures", []),
        "translation_required": compatibility.get("translation_required"),
    }


def _discover_reference_roots(search_roots: list[Path]) -> dict[str, list[str]]:
    references = {"helios": [], "awsim": [], "carla": []}
    for root in search_roots:
        if not root.exists():
            continue
        for relative, key in (
            ("third_party/helios", "helios"),
            ("_reference_repos/awsim", "awsim"),
            ("_reference_repos/carla", "carla"),
        ):
            candidate = (root / relative).resolve()
            if candidate.exists():
                references[key].append(str(candidate))
        for candidate in _iter_with_depth(root, max_depth=5):
            if not candidate.is_dir():
                continue
            if (
                (candidate / "python/pyhelios").exists()
                or (candidate / "src/visualhelios").exists()
                or (candidate / "wiki-repo/helios.wiki").exists()
            ):
                references["helios"].append(str(candidate.resolve()))
            if (
                (candidate / "Packages/manifest.json").exists()
                and (candidate / "ProjectSettings/ProjectVersion.txt").exists()
            ) or (candidate / "AWSIM.slnx").exists():
                references["awsim"].append(str(candidate.resolve()))
            if (candidate / "PythonAPI").exists() or (candidate / "CarlaUE4.uproject").exists():
                references["carla"].append(str(candidate.resolve()))
    return {key: sorted(set(values)) for key, values in references.items()}


def _candidate_paths_for_repo(repo_root: Path) -> dict[str, list[Path]]:
    return {
        "helios": [
            repo_root / "third_party/helios/build/helios++",
            repo_root / "third_party/helios/build/Release/helios++",
            repo_root / "third_party/helios/build/Debug/helios++",
            repo_root / "third_party/helios/build/pyhelios/bin/helios++",
            repo_root / "third_party/helios/helios++",
        ],
        "awsim": [
            repo_root / "third_party/awsim/AWSIM.app/Contents/MacOS/AWSIM",
            repo_root / "third_party/awsim/AWSIM.x86_64",
            repo_root / "third_party/awsim/AWSIM-Demo.x86_64",
            repo_root / "third_party/awsim/AWSIM-Demo-Lightweight.x86_64",
            repo_root / "third_party/runtime_backends/awsim/expanded/AWSIM.x86_64",
            repo_root / "third_party/runtime_backends/awsim/expanded/AWSIM-Demo.x86_64",
            repo_root / "third_party/runtime_backends/awsim/expanded/AWSIM-Demo-Lightweight.x86_64",
            repo_root / "third_party/runtime_backends/awsim/expanded/AWSIM-Demo/AWSIM-Demo.x86_64",
            repo_root / "third_party/runtime_backends/awsim/expanded/AWSIM-Demo-Lightweight/AWSIM-Demo-Lightweight.x86_64",
        ],
        "carla": [
            repo_root / "third_party/carla/CarlaUE4.sh",
            repo_root / "third_party/carla/CarlaUE4.app/Contents/MacOS/CarlaUE4",
            repo_root / "third_party/carla/CarlaUE5.sh",
            repo_root / "third_party/carla/CarlaUE5.app/Contents/MacOS/CarlaUE5",
            repo_root / "third_party/carla/CarlaUnreal.sh",
            repo_root / "third_party/carla/CarlaUnreal.exe",
            repo_root / "third_party/runtime_backends/carla/expanded/CarlaUE4.sh",
            repo_root / "third_party/runtime_backends/carla/expanded/CarlaUE5.sh",
            repo_root / "third_party/runtime_backends/carla/expanded/CarlaUnreal.sh",
            repo_root / "third_party/runtime_backends/carla/expanded/CARLA_UE5/CarlaUnreal.sh",
            repo_root / "third_party/runtime_backends/carla/expanded/CARLA_UE5/CarlaUE5.sh",
        ],
    }


def _load_stage_selected_candidate(repo_root: Path, backend: str) -> Path | None:
    summary_path = repo_root / "third_party" / "runtime_backends" / backend / "renderer_backend_package_stage.json"
    if not summary_path.exists():
        return None
    try:
        payload = _load_json(summary_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    staging = payload.get("staging", {})
    if not isinstance(staging, dict):
        return None
    selected_path = staging.get("selected_executable_path")
    if not isinstance(selected_path, str) or not selected_path.strip():
        return None
    candidate = _resolve_runtime_path(selected_path)
    return candidate if candidate.exists() else None


def _iter_with_depth(root: Path, max_depth: int) -> list[Path]:
    if not root.exists():
        return []
    results: list[Path] = []
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        results.append(current)
        if depth >= max_depth or not current.is_dir():
            continue
        try:
            children = list(current.iterdir())
        except OSError:
            continue
        for child in children:
            stack.append((child, depth + 1))
    return results


def _scan_named_candidates(
    *,
    search_roots: list[Path],
    names: set[str],
    max_depth: int = 5,
) -> list[Path]:
    matches: list[Path] = []
    lowered = {name.lower() for name in names}
    for root in search_roots:
        for path in _iter_with_depth(root, max_depth=max_depth):
            if path.is_file() and path.name.lower() in lowered:
                matches.append(path.resolve())
    unique: list[Path] = []
    seen: set[str] = set()
    for path in matches:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _scan_archive_candidates(
    *,
    search_roots: list[Path],
    tokens: set[str],
    suffixes: tuple[str, ...] = (".zip", ".tar.gz", ".tgz"),
    max_depth: int = 4,
) -> list[Path]:
    matches: list[Path] = []
    lowered_tokens = {token.lower() for token in tokens}
    lowered_suffixes = tuple(suffix.lower() for suffix in suffixes)
    for root in search_roots:
        for path in _iter_with_depth(root, max_depth=max_depth):
            if not path.is_file():
                continue
            name = path.name.lower()
            if not name.endswith(lowered_suffixes):
                continue
            if any(token in name for token in lowered_tokens):
                matches.append(path.resolve())
    unique: list[Path] = []
    seen: set[str] = set()
    for path in matches:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _select_preferred_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for candidate in candidates:
        if (
            candidate.get("exists")
            and candidate.get("executable")
            and candidate.get("host_compatible")
        ):
            return candidate
    for candidate in candidates:
        if candidate.get("exists") and candidate.get("executable"):
            return candidate
    for candidate in candidates:
        if candidate.get("exists"):
            return candidate
    return None


def _docker_daemon_status() -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["docker", "info"],
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return False, "docker CLI is not installed."
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        return False, stderr if stderr else "docker daemon is not reachable."
    return True, ""


def _docker_image_present(image: str) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["docker", "image", "inspect", image],
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return False, "docker CLI is not installed."
    if proc.returncode != 0:
        try:
            list_proc = subprocess.run(
                ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
                text=True,
                capture_output=True,
                check=False,
            )
        except FileNotFoundError:
            return False, "docker CLI is not installed."
        if list_proc.returncode == 0:
            tags = {line.strip() for line in list_proc.stdout.splitlines() if line.strip()}
            if image in tags:
                return True, "image found via docker images listing."
        stderr = proc.stderr.strip()
        return False, stderr if stderr else f"docker image not found: {image}"
    return True, ""


def _inspect_helios_docker_runtime() -> dict[str, Any]:
    image = os.getenv("HELIOS_DOCKER_IMAGE", _DEFAULT_HELIOS_DOCKER_IMAGE).strip()
    binary = os.getenv("HELIOS_DOCKER_BINARY", _DEFAULT_HELIOS_DOCKER_BINARY).strip()
    mount_point = os.getenv("HELIOS_DOCKER_MOUNT_POINT", _DEFAULT_HELIOS_DOCKER_MOUNT_POINT).strip()
    daemon_ready, daemon_message = _docker_daemon_status()
    image_ready = False
    image_message = "docker daemon unavailable."
    if daemon_ready:
        image_ready, image_message = _docker_image_present(image)
    return {
        "image": image,
        "binary": binary,
        "mount_point": mount_point,
        "daemon_ready": daemon_ready,
        "daemon_message": daemon_message,
        "image_ready": image_ready,
        "image_message": image_message,
        "ready": daemon_ready and image_ready,
    }


def _discover_backend_candidates(
    *,
    backend: str,
    repo_root: Path,
    search_roots: list[Path],
    env_var: str,
    repo_candidates: list[Path],
    scanned_names: set[str],
    reference_roots: list[str],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    env_value = os.getenv(env_var, "").strip()
    if env_value:
        candidates.append(
            _path_record(_resolve_runtime_path(env_value), origin=f"env:{env_var}", kind="env")
        )
    stage_candidate = _load_stage_selected_candidate(
        repo_root,
        backend,
    )
    if stage_candidate is not None:
        candidates.append(
            _path_record(stage_candidate, origin="stage-summary", kind="candidate")
        )
    for path in repo_candidates:
        candidates.append(_path_record(path.resolve(), origin="repo-default", kind="candidate"))
    for path in _scan_named_candidates(search_roots=search_roots, names=scanned_names):
        candidates.append(_path_record(path, origin="search-root", kind="candidate"))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate["path"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    selected = _select_preferred_candidate(deduped)
    return {
        "env_var": env_var,
        "selected_path": selected.get("path") if selected else None,
        "selected_origin": selected.get("origin") if selected else None,
        "ready": bool(selected and selected.get("executable")),
        "host_compatible_ready": bool(selected and selected.get("host_compatible")),
        "source_only": bool(reference_roots) and not bool(selected and selected.get("executable")),
        "reference_roots": list(reference_roots),
        "candidates": deduped,
    }


def _render_env_file(summary: dict[str, Any]) -> str:
    selection = summary.get("selection", {})
    readiness = summary.get("readiness", {})
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by renderer_backend_local_setup.py",
        "# Source this file before running smoke presets.",
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
            f"# helios_binary_ready={readiness.get('helios_binary_ready', False)}",
            f"# helios_binary_host_compatible={readiness.get('helios_binary_host_compatible', False)}",
            f"# helios_docker_ready={readiness.get('helios_docker_ready', False)}",
            f"# awsim_host_compatible={readiness.get('awsim_host_compatible', False)}",
            f"# awsim_smoke_ready={readiness.get('awsim_smoke_ready', False)}",
            f"# carla_host_compatible={readiness.get('carla_host_compatible', False)}",
            f"# carla_smoke_ready={readiness.get('carla_smoke_ready', False)}",
            "#",
            "# Example:",
            "# source artifacts/renderer_backend_local_setup/renderer_backend_local.env.sh",
            "# python3 scripts/discover_renderer_backend_local_env.py --probe-helios-docker-demo",
            "# python3 scripts/run_renderer_backend_smoke.py --config configs/renderer_backend_smoke.awsim.local.example.json --backend awsim",
            "# python3 scripts/run_renderer_backend_smoke.py --config configs/renderer_backend_smoke.awsim.local.docker.example.json --backend awsim",
            "",
        ]
    )
    return "\n".join(lines)


def _run_hybrid_config(config_path: Path) -> dict[str, Any]:
    cfg = _load_json(config_path)
    mode = BackendMode(cfg.get("mode", BackendMode.HYBRID_AUTO.value))
    options = dict(cfg.get("options", {}))
    if cfg.get("helios_runtime") and "helios_runtime" not in options:
        options["helios_runtime"] = cfg["helios_runtime"]
    request = SensorSimRequest(
        scenario_path=Path(cfg["scenario_path"]),
        output_dir=Path(cfg["output_dir"]),
        sensor_profile=cfg.get("sensor_profile", "default"),
        seed=int(cfg.get("seed", 0)),
        options=options,
    )
    request.output_dir.mkdir(parents=True, exist_ok=True)
    orchestrator = HybridOrchestrator(
        helios=HeliosAdapter(
            helios_bin=Path(cfg["helios_bin"]) if cfg.get("helios_bin") else None
        ),
        native=NativePhysicsBackend(),
    )
    result = orchestrator.run(request, mode)
    return {
        "backend": result.backend,
        "success": result.success,
        "message": result.message,
        "artifacts": {key: str(value) for key, value in result.artifacts.items()},
        "metrics": result.metrics,
    }


def _existing_path_or_none(path: Path) -> str | None:
    return str(path.resolve()) if path.exists() else None


def _host_platform_summary() -> dict[str, str]:
    return {
        "system": platform.system(),
        "machine": platform.machine(),
    }


def _build_acquisition_hints(
    *,
    repo_root: Path,
    search_roots: list[Path],
    backends: dict[str, Any],
    runtimes: dict[str, Any],
    readiness: dict[str, Any],
) -> dict[str, Any]:
    host = _host_platform_summary()
    system = host["system"]
    helios = backends["helios"]
    awsim = backends["awsim"]
    carla = backends["carla"]
    helios_docker = runtimes["helios_docker"]
    awsim_download_candidates = [
        str(path)
        for path in _scan_archive_candidates(
            search_roots=search_roots,
            tokens={"awsim-demo", "awsim-demo-lightweight", "awsim-demo-openscenario"},
        )
    ]
    carla_download_candidates = [
        str(path)
        for path in _scan_archive_candidates(
            search_roots=search_roots,
            tokens={"carla_ue5", "carla-0.", "carla_0.", "carla"},
            suffixes=(".tar.gz", ".tgz", ".zip"),
        )
    ]

    helios_hints = {
        "status": (
            "docker_ready"
            if readiness.get("helios_docker_ready")
            else "binary_incompatible_host"
            if helios.get("ready") and not helios.get("host_compatible_ready")
            else "binary_ready"
            if readiness.get("helios_binary_ready")
            else "missing_runtime"
        ),
        "recommended_runtime": (
            "docker"
            if readiness.get("helios_docker_ready")
            else "binary"
            if readiness.get("helios_binary_ready")
            else "docker_build_or_binary_build"
        ),
        "next_actions": [
            {
                "type": "docker_build",
                "command": "bash scripts/docker_build_helios_cli.sh heliosplusplus:cli",
                "source": _existing_path_or_none(repo_root / "scripts/docker_build_helios_cli.sh"),
            },
            {
                "type": "binary_build",
                "command": "bash scripts/setup_helios.sh",
                "source": _existing_path_or_none(repo_root / "scripts/setup_helios.sh"),
            },
            {
                "type": "docker_demo",
                "command": "bash scripts/run_hybrid_docker_demo.sh configs/hybrid_sensor_sim.helios_docker.json",
                "source": _existing_path_or_none(repo_root / "scripts/run_hybrid_docker_demo.sh"),
            },
        ],
        "reference_docs": [
            _existing_path_or_none(repo_root / "README.md"),
            _existing_path_or_none(repo_root / "configs/hybrid_sensor_sim.helios_docker.json"),
        ],
        "docker": {
            "image": helios_docker.get("image"),
            "ready": helios_docker.get("ready"),
            "message": helios_docker.get("image_message") or helios_docker.get("daemon_message"),
        },
    }

    awsim_hints = {
        "status": (
            "runtime_incompatible_host"
            if awsim.get("ready") and not awsim.get("host_compatible_ready")
            else "runtime_available"
            if awsim.get("ready")
            else "source_only"
            if awsim.get("source_only")
            else "missing_runtime"
        ),
        "platform_supported": system == "Linux",
        "platform_note": (
            "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+."
            if system != "Linux"
            else "Ubuntu 22.04 with NVIDIA RTX is the documented target."
        ),
        "recommended_executable_name": "AWSIM-Demo.x86_64",
        "download_options": [
            {
                "name": "AWSIM-Demo.zip",
                "url": "https://github.com/tier4/AWSIM/releases/download/v2.0.1/AWSIM-Demo.zip",
            },
            {
                "name": "AWSIM-Demo-Lightweight.zip",
                "url": "https://github.com/tier4/AWSIM/releases/download/v2.0.1/AWSIM-Demo-Lightweight.zip",
            },
        ],
        "next_actions": [
            "Download an AWSIM demo package and extract it.",
            f"Run python3 scripts/acquire_renderer_backend_package.py --backend awsim --setup-summary {shlex.quote(str((repo_root / 'artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json').resolve()))} to download and stage the package automatically.",
            "Mark AWSIM-Demo.x86_64 executable and export AWSIM_BIN to that path.",
            "Re-run local discovery and then run the docker-backed AWSIM smoke preset.",
        ],
        "reference_docs": [
            _existing_path_or_none(
                Path("/Users/seongcheoljeong/Documents/Autonomy-E2E/_reference_repos/awsim/docs/Downloads/index.md")
            ),
            _existing_path_or_none(
                Path("/Users/seongcheoljeong/Documents/Autonomy-E2E/_reference_repos/awsim/docs/GettingStarted/QuickStartDemo/index.md")
            ),
        ],
        "local_download_candidates": awsim_download_candidates,
    }

    carla_hints = {
        "status": (
            "runtime_incompatible_host"
            if carla.get("ready") and not carla.get("host_compatible_ready")
            else "runtime_available"
            if carla.get("ready")
            else "source_only"
            if carla.get("source_only")
            else "missing_runtime"
        ),
        "platform_supported": system in {"Linux", "Windows"},
        "platform_note": (
            "CARLA UE5 package docs target Ubuntu 22.04 or Windows 11. On this host, use a Linux or Windows runner."
            if system not in {"Linux", "Windows"}
            else "CARLA UE5 docs target Ubuntu 22.04 or Windows 11."
        ),
        "download_options": [
            {
                "name": "CARLA 0.10.0 release page",
                "url": "https://github.com/carla-simulator/carla/releases/tag/0.10.0",
            },
            {
                "name": "CARLA Nightly Build (Linux)",
                "url": "https://s3.us-east-005.backblazeb2.com/carla-releases/Linux/Dev/CARLA_UE5_Latest.tar.gz",
            },
            {
                "name": "CARLA Docker image",
                "command": "docker pull carlasim/carla:0.10.0",
            },
        ],
        "next_actions": [
            "Use a packaged CARLA release or Linux Docker image on a supported Linux/Windows runner.",
            f"Run python3 scripts/acquire_renderer_backend_package.py --backend carla --setup-summary {shlex.quote(str((repo_root / 'artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json').resolve()))} to download and stage the package automatically.",
            "If building from source, run ./CarlaSetup.sh --interactive in a Linux CARLA UE5 checkout.",
            "Export CARLA_BIN to CarlaUnreal.sh or the packaged launcher path and re-run local discovery.",
        ],
        "reference_docs": [
            _existing_path_or_none(
                Path("/Users/seongcheoljeong/Documents/Autonomy-E2E/_reference_repos/carla/Docs/start_quickstart.md")
            ),
            _existing_path_or_none(
                Path("/Users/seongcheoljeong/Documents/Autonomy-E2E/_reference_repos/carla/Docs/download.md")
            ),
            _existing_path_or_none(
                Path("/Users/seongcheoljeong/Documents/Autonomy-E2E/_reference_repos/carla/Docs/build_docker.md")
            ),
        ],
        "local_download_candidates": carla_download_candidates,
    }

    return {
        "host_platform": host,
        "helios": helios_hints,
        "awsim": awsim_hints,
        "carla": carla_hints,
    }


def build_renderer_backend_local_setup(
    *,
    repo_root: Path,
    search_roots: list[Path] | None = None,
    output_dir: Path | None = None,
    include_default_search_roots: bool = True,
    probe_helios_docker_demo: bool = False,
    helios_docker_probe_config: Path | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    extra_roots = [_resolve_runtime_path(path) for path in (search_roots or [])]
    default_roots = [repo_root]
    if include_default_search_roots:
        default_roots.extend(
            [
                repo_root.parent,
                Path.home() / "Documents",
                Path.home() / "Downloads",
                Path("/Applications"),
            ]
        )
    all_search_roots: list[Path] = []
    seen_roots: set[str] = set()
    for root in [*extra_roots, *default_roots]:
        resolved = root.resolve()
        key = str(resolved)
        if key in seen_roots:
            continue
        seen_roots.add(key)
        all_search_roots.append(resolved)

    reference_roots = _discover_reference_roots(all_search_roots)
    repo_candidates = _candidate_paths_for_repo(repo_root)
    helios_docker = _inspect_helios_docker_runtime()
    helios = _discover_backend_candidates(
        backend="helios",
        repo_root=repo_root,
        search_roots=all_search_roots,
        env_var="HELIOS_BIN",
        repo_candidates=repo_candidates["helios"],
        scanned_names={"helios++", "helios"},
        reference_roots=reference_roots["helios"],
    )
    awsim = _discover_backend_candidates(
        backend="awsim",
        repo_root=repo_root,
        search_roots=all_search_roots,
        env_var="AWSIM_BIN",
        repo_candidates=repo_candidates["awsim"],
        scanned_names=_AWSIM_EXECUTABLE_NAMES,
        reference_roots=reference_roots["awsim"],
    )
    carla = _discover_backend_candidates(
        backend="carla",
        repo_root=repo_root,
        search_roots=all_search_roots,
        env_var="CARLA_BIN",
        repo_candidates=repo_candidates["carla"],
        scanned_names=_CARLA_EXECUTABLE_NAMES,
        reference_roots=reference_roots["carla"],
    )

    selection = {
        "HELIOS_BIN": helios.get("selected_path"),
        "HELIOS_DOCKER_IMAGE": helios_docker["image"],
        "HELIOS_DOCKER_BINARY": helios_docker["binary"],
        "AWSIM_BIN": awsim.get("selected_path"),
        "AWSIM_RENDERER_MAP": os.getenv("AWSIM_RENDERER_MAP", _DEFAULT_BACKEND_MAPS["awsim"]),
        "CARLA_BIN": carla.get("selected_path"),
        "CARLA_RENDERER_MAP": os.getenv("CARLA_RENDERER_MAP", _DEFAULT_BACKEND_MAPS["carla"]),
    }
    readiness = {
        "helios_binary_ready": bool(helios.get("ready")),
        "helios_binary_host_compatible": bool(helios.get("host_compatible_ready")),
        "helios_docker_ready": bool(helios_docker.get("ready")),
        "awsim_ready": bool(awsim.get("ready")),
        "awsim_host_compatible": bool(awsim.get("host_compatible_ready")),
        "carla_ready": bool(carla.get("ready")),
        "carla_host_compatible": bool(carla.get("host_compatible_ready")),
    }
    readiness["helios_ready"] = readiness["helios_binary_host_compatible"] or readiness["helios_docker_ready"]
    readiness["awsim_smoke_ready_binary"] = readiness["helios_binary_host_compatible"] and readiness["awsim_host_compatible"]
    readiness["awsim_smoke_ready_docker"] = readiness["helios_docker_ready"] and readiness["awsim_host_compatible"]
    readiness["carla_smoke_ready_binary"] = readiness["helios_binary_host_compatible"] and readiness["carla_host_compatible"]
    readiness["carla_smoke_ready_docker"] = readiness["helios_docker_ready"] and readiness["carla_host_compatible"]
    readiness["awsim_smoke_ready"] = (
        readiness["awsim_smoke_ready_binary"] or readiness["awsim_smoke_ready_docker"]
    )
    readiness["carla_smoke_ready"] = (
        readiness["carla_smoke_ready_binary"] or readiness["carla_smoke_ready_docker"]
    )

    issues: list[str] = []
    if not readiness["helios_binary_ready"] and not readiness["helios_docker_ready"]:
        issues.append("HELIOS binary is not resolved.")
        if not helios_docker["daemon_ready"]:
            issues.append(f"HELIOS docker runtime unavailable: {helios_docker['daemon_message']}")
        elif not helios_docker["image_ready"]:
            issues.append(f"HELIOS docker image unavailable: {helios_docker['image_message']}")
    elif readiness["helios_binary_ready"] and not readiness["helios_binary_host_compatible"]:
        issues.append("HELIOS binary is resolved but incompatible with the current host.")
    if not readiness["awsim_ready"]:
        issues.append("AWSIM runtime binary is not resolved.")
    elif not readiness["awsim_host_compatible"]:
        issues.append("AWSIM runtime binary is resolved but incompatible with the current host.")
    if not readiness["carla_ready"]:
        issues.append("CARLA runtime binary is not resolved.")
    elif not readiness["carla_host_compatible"]:
        issues.append("CARLA runtime binary is resolved but incompatible with the current host.")

    output_root = (
        _resolve_runtime_path(output_dir)
        if output_dir is not None
        else (repo_root / "artifacts" / "renderer_backend_local_setup").resolve()
    )
    env_path = output_root / "renderer_backend_local.env.sh"
    summary_path = output_root / "renderer_backend_local_setup.json"
    probe_path = output_root / "helios_docker_probe.json"
    commands = {
        "helios_docker_demo": "python3 scripts/discover_renderer_backend_local_env.py --probe-helios-docker-demo",
        "awsim_acquire": (
            "python3 scripts/acquire_renderer_backend_package.py "
            f"--backend awsim --setup-summary {shlex.quote(str(summary_path))}"
        ),
        "carla_acquire": (
            "python3 scripts/acquire_renderer_backend_package.py "
            f"--backend carla --setup-summary {shlex.quote(str(summary_path))}"
        ),
        "awsim_smoke_binary": (
            f"source {shlex.quote(str(env_path))} && "
            "python3 scripts/run_renderer_backend_smoke.py "
            "--config configs/renderer_backend_smoke.awsim.local.example.json --backend awsim"
        ),
        "awsim_smoke_docker": (
            f"source {shlex.quote(str(env_path))} && "
            "python3 scripts/run_renderer_backend_smoke.py "
            "--config configs/renderer_backend_smoke.awsim.local.docker.example.json --backend awsim"
        ),
        "carla_smoke_binary": (
            f"source {shlex.quote(str(env_path))} && "
            "python3 scripts/run_renderer_backend_smoke.py "
            "--config configs/renderer_backend_smoke.carla.local.example.json --backend carla"
        ),
        "carla_smoke_docker": (
            f"source {shlex.quote(str(env_path))} && "
            "python3 scripts/run_renderer_backend_smoke.py "
            "--config configs/renderer_backend_smoke.carla.local.docker.example.json --backend carla"
        ),
    }
    commands["awsim_smoke"] = (
        commands["awsim_smoke_binary"]
        if readiness["awsim_smoke_ready_binary"]
        else commands["awsim_smoke_docker"]
    )
    commands["carla_smoke"] = (
        commands["carla_smoke_binary"]
        if readiness["carla_smoke_ready_binary"]
        else commands["carla_smoke_docker"]
    )
    probes: dict[str, Any] = {}
    if probe_helios_docker_demo:
        probe_config = _resolve_runtime_path(
            helios_docker_probe_config
            if helios_docker_probe_config is not None
            else repo_root / "configs/hybrid_sensor_sim.helios_docker.json"
        )
        probe_summary: dict[str, Any] = {
            "enabled": True,
            "config_path": str(probe_config),
            "ready": bool(helios_docker.get("ready")),
        }
        if helios_docker.get("ready"):
            try:
                probe_summary.update(_run_hybrid_config(probe_config))
            except Exception as exc:
                probe_summary.update(
                    {
                        "success": False,
                        "message": str(exc),
                        "artifacts": {},
                        "metrics": {},
                    }
                )
        else:
            probe_summary.update(
                {
                    "success": False,
                    "message": "HELIOS docker runtime is not ready.",
                    "artifacts": {},
                    "metrics": {},
                }
            )
        _write_json(probe_path, probe_summary)
        probes["helios_docker_demo"] = probe_summary
    acquisition_hints = _build_acquisition_hints(
        repo_root=repo_root,
        search_roots=all_search_roots,
        backends={
            "helios": helios,
            "awsim": awsim,
            "carla": carla,
        },
        runtimes={
            "helios_docker": helios_docker,
        },
        readiness=readiness,
    )
    return {
        "search_roots": [str(path) for path in all_search_roots],
        "backends": {
            "helios": helios,
            "awsim": awsim,
            "carla": carla,
        },
        "runtimes": {
            "helios_docker": helios_docker,
        },
        "probes": probes,
        "acquisition_hints": acquisition_hints,
        "selection": selection,
        "readiness": readiness,
        "issues": issues,
        "commands": commands,
        "artifacts": {
            "summary_path": str(summary_path),
            "env_path": str(env_path),
            "helios_docker_probe_path": str(probe_path),
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    output_dir = _resolve_runtime_path(args.output_dir)
    summary = build_renderer_backend_local_setup(
        repo_root=repo_root,
        search_roots=[_resolve_runtime_path(path) for path in args.search_root],
        output_dir=output_dir,
        include_default_search_roots=not args.no_default_search_roots,
        probe_helios_docker_demo=args.probe_helios_docker_demo,
        helios_docker_probe_config=_resolve_runtime_path(args.helios_docker_probe_config),
    )
    summary_path = (
        _resolve_runtime_path(args.summary_path)
        if args.summary_path is not None
        else Path(summary["artifacts"]["summary_path"])
    )
    env_path = (
        _resolve_runtime_path(args.env_path)
        if args.env_path is not None
        else Path(summary["artifacts"]["env_path"])
    )
    summary["artifacts"]["summary_path"] = str(summary_path)
    summary["artifacts"]["env_path"] = str(env_path)
    _write_json(summary_path, summary)
    _write_text(env_path, _render_env_file(summary))
    print(json.dumps(summary, indent=2))
    return 0 if summary["readiness"]["awsim_smoke_ready"] or summary["readiness"]["carla_smoke_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
