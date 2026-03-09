from __future__ import annotations

import argparse
from datetime import datetime, timezone
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
from hybrid_sensor_sim.tools.renderer_backend_linux_handoff_selftest import (
    run_renderer_backend_linux_handoff_selftest,
)
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest


_DEFAULT_BACKEND_MAPS = {
    "awsim": "SampleMap",
    "carla": "Town03",
}

_DEFAULT_HELIOS_DOCKER_IMAGE = "heliosplusplus:cli"
_DEFAULT_HELIOS_DOCKER_BINARY = "/home/jovyan/helios/build/helios++"
_DEFAULT_HELIOS_DOCKER_MOUNT_POINT = "/workspace"
_DEFAULT_CARLA_DOCKER_IMAGE = "carlasim/carla:0.10.0"
_DEFAULT_CARLA_DOCKER_PLATFORM = "linux/amd64"
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
_DISCOVERY_IGNORE_PATH_TOKENS = {
    "renderer_backend_workflow_selftest_probe",
    "backend_workflow_selftest_probe",
    "renderer_backend_package_workflow_selftest_probe",
    "backend_package_workflow_selftest_probe",
    "renderer_backend_linux_handoff_selftest",
    "linux_handoff_docker_selftest_probe",
    "workflow_docker_preflight_demo",
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
    parser.add_argument(
        "--probe-linux-handoff-docker-selftest",
        action="store_true",
        help="Run the synthetic Linux handoff Docker self-test and store the result summary.",
    )
    parser.add_argument(
        "--probe-linux-handoff-docker-selftest-execute",
        action="store_true",
        help="When probing the Linux handoff Docker self-test, execute the extracted handoff script too.",
    )
    parser.add_argument(
        "--probe-backend-workflow-selftest",
        action="store_true",
        help="Run the end-to-end backend workflow self-test and store the result summary.",
    )
    parser.add_argument(
        "--workflow-selftest-backend",
        choices=("awsim", "carla"),
        default="awsim",
        help="Backend flavor used by --probe-backend-workflow-selftest.",
    )
    parser.add_argument(
        "--probe-backend-workflow-selftest-execute",
        action="store_true",
        help="When probing the backend workflow self-test, execute the extracted Docker handoff script too.",
    )
    parser.add_argument(
        "--probe-backend-package-workflow-selftest",
        action="store_true",
        help="Run the packaged backend workflow self-test and store the result summary.",
    )
    parser.add_argument(
        "--probe-carla-docker-pull",
        action="store_true",
        help="Attempt to pull the configured CARLA Docker image and store the result summary.",
    )
    parser.add_argument(
        "--probe-docker-storage",
        action="store_true",
        help="Probe whether the local Docker image store is usable and store the result summary.",
    )
    parser.add_argument(
        "--package-workflow-selftest-backend",
        choices=("awsim", "carla"),
        default="awsim",
        help="Backend flavor used by --probe-backend-package-workflow-selftest.",
    )
    parser.add_argument(
        "--package-workflow-selftest-archive-source",
        choices=("local_candidate", "download_url"),
        default="local_candidate",
        help="Archive source mode used by --probe-backend-package-workflow-selftest.",
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _should_ignore_discovery_path(path: Path) -> bool:
    lowered_parts = {part.lower() for part in path.resolve().parts}
    return any(token in lowered_parts for token in _DISCOVERY_IGNORE_PATH_TOKENS)


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
        allow_ignored_under_root = _should_ignore_discovery_path(root)
        for path in _iter_with_depth(root, max_depth=max_depth):
            if not allow_ignored_under_root and _should_ignore_discovery_path(path):
                continue
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
        allow_ignored_under_root = _should_ignore_discovery_path(root)
        for path in _iter_with_depth(root, max_depth=max_depth):
            if not allow_ignored_under_root and _should_ignore_discovery_path(path):
                continue
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


def _inspect_carla_docker_runtime() -> dict[str, Any]:
    image = os.getenv("CARLA_DOCKER_IMAGE", _DEFAULT_CARLA_DOCKER_IMAGE).strip()
    platform_name = os.getenv("CARLA_DOCKER_PLATFORM", _DEFAULT_CARLA_DOCKER_PLATFORM).strip()
    daemon_ready, daemon_message = _docker_daemon_status()
    image_ready = False
    image_message = "docker daemon unavailable."
    if daemon_ready:
        image_ready, image_message = _docker_image_present(image)
    return {
        "image": image,
        "platform": platform_name,
        "daemon_ready": daemon_ready,
        "daemon_message": daemon_message,
        "image_ready": image_ready,
        "image_message": image_message,
        "ready": daemon_ready and image_ready,
    }


def _run_carla_docker_pull_probe(*, image: str, platform_name: str) -> dict[str, Any]:
    command = ["docker", "pull", "--platform", platform_name, image]
    try:
        proc = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return {
            "generated_at_utc": _format_utc(_utc_now()),
            "image": image,
            "platform": platform_name,
            "command": command,
            "success": False,
            "return_code": 127,
            "stdout": "",
            "stderr": "docker CLI is not installed.",
        }
    return {
        "generated_at_utc": _format_utc(_utc_now()),
        "image": image,
        "platform": platform_name,
        "command": command,
        "success": proc.returncode == 0,
        "return_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _run_docker_storage_probe() -> dict[str, Any]:
    command = ["docker", "system", "df"]
    try:
        proc = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return {
            "generated_at_utc": _format_utc(_utc_now()),
            "command": command,
            "success": False,
            "return_code": 127,
            "stdout": "",
            "stderr": "docker CLI is not installed.",
        }
    return {
        "generated_at_utc": _format_utc(_utc_now()),
        "command": command,
        "success": proc.returncode == 0,
        "return_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _classify_docker_storage_probe(probe: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(probe, dict) or not probe:
        return None, None
    if probe.get("success") is True:
        return "healthy", None
    stderr = str(probe.get("stderr") or "").strip()
    lowered = stderr.lower()
    if "docker cli is not installed" in lowered:
        return "docker_cli_missing", stderr
    if "daemon is not reachable" in lowered:
        return "daemon_unreachable", stderr
    if "io.containerd.metadata.v1.bolt/meta.db" in lowered:
        return "image_store_corrupt", stderr
    if "blob sha256" in lowered and "input/output error" in lowered:
        return "content_store_corrupt", stderr
    if "input/output error" in lowered:
        return "storage_io_error", stderr
    return "probe_failed", stderr or None


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
        "CARLA_DOCKER_IMAGE",
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
            f"# carla_docker_ready={readiness.get('carla_docker_ready', False)}",
            f"# carla_smoke_ready={readiness.get('carla_smoke_ready', False)}",
            "#",
            "# Example:",
            "# source artifacts/renderer_backend_local_setup/renderer_backend_local.env.sh",
            "# python3 scripts/discover_renderer_backend_local_env.py --probe-helios-docker-demo",
            "# python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest --probe-linux-handoff-docker-selftest-execute",
            "# python3 scripts/discover_renderer_backend_local_env.py --probe-backend-workflow-selftest --workflow-selftest-backend awsim",
            "# python3 scripts/discover_renderer_backend_local_env.py --probe-backend-package-workflow-selftest --package-workflow-selftest-backend awsim",
            "# python3 scripts/run_renderer_backend_smoke.py --config configs/renderer_backend_smoke.awsim.local.example.json --backend awsim",
            "# python3 scripts/run_renderer_backend_smoke.py --config configs/renderer_backend_smoke.awsim.local.docker.example.json --backend awsim",
            "",
        ]
    )
    return "\n".join(lines)


def _probe_success(probes: dict[str, Any], key: str) -> bool | None:
    probe = probes.get(key) if isinstance(probes, dict) else None
    if not isinstance(probe, dict) or not probe:
        return None
    success = probe.get("success")
    if isinstance(success, bool):
        return success
    return None


def _build_probe_readiness(summary: dict[str, Any]) -> dict[str, Any]:
    probes = summary.get("probes", {})
    backend_workflow_probe = probes.get("backend_workflow_selftest", {}) if isinstance(probes, dict) else {}
    backend_package_workflow_probe = (
        probes.get("backend_package_workflow_selftest", {}) if isinstance(probes, dict) else {}
    )
    docker_storage_probe = probes.get("docker_storage", {}) if isinstance(probes, dict) else {}
    docker_storage_status, _ = _classify_docker_storage_probe(
        docker_storage_probe if isinstance(docker_storage_probe, dict) else None
    )
    return {
        "helios_docker_demo_ready": _probe_success(probes, "helios_docker_demo"),
        "linux_handoff_docker_selftest_ready": _probe_success(probes, "linux_handoff_docker_selftest"),
        "backend_workflow_selftest_ready": _probe_success(probes, "backend_workflow_selftest"),
        "backend_workflow_status": (
            backend_workflow_probe.get("workflow_status")
            if isinstance(backend_workflow_probe, dict)
            else None
        ),
        "carla_docker_pull_ready": _probe_success(probes, "carla_docker_pull"),
        "docker_storage_ready": _probe_success(probes, "docker_storage"),
        "docker_storage_status": docker_storage_status,
        "backend_package_workflow_selftest_ready": _probe_success(
            probes,
            "backend_package_workflow_selftest",
        ),
        "backend_package_workflow_status": (
            backend_package_workflow_probe.get("workflow_status")
            if isinstance(backend_package_workflow_probe, dict)
            else None
        ),
    }


def _build_workflow_paths(readiness: dict[str, Any], probe_readiness: dict[str, Any]) -> dict[str, Any]:
    helios_docker_ready = bool(readiness.get("helios_docker_ready"))
    handoff_selftest_ready = probe_readiness.get("linux_handoff_docker_selftest_ready") is True
    workflow_selftest_ready = probe_readiness.get("backend_workflow_selftest_ready") is True
    package_workflow_selftest_ready = (
        probe_readiness.get("backend_package_workflow_selftest_ready") is True
    )
    return {
        "helios_docker_available": helios_docker_ready,
        "carla_docker_available": bool(readiness.get("carla_docker_ready")),
        "linux_handoff_docker_path_ready": helios_docker_ready and handoff_selftest_ready,
        "backend_workflow_path_ready": workflow_selftest_ready,
        "package_workflow_path_ready": package_workflow_selftest_ready,
        "local_backend_smoke_ready": bool(
            readiness.get("awsim_smoke_ready") or readiness.get("carla_smoke_ready")
        ),
    }


def _render_local_setup_report(summary: dict[str, Any], summary_path: Path) -> str:
    readiness = summary.get("readiness", {})
    probe_readiness = summary.get("probe_readiness", {})
    workflow_paths = summary.get("workflow_paths", {})
    runtime_strategy = summary.get("runtime_strategy", {})
    selection = summary.get("selection", {})
    commands = summary.get("commands", {})
    issues = summary.get("issues", [])
    lines = [
        "# Renderer Backend Local Setup Report",
        "",
        f"- summary_path: `{summary_path}`",
        f"- generated_at_utc: `{summary.get('generated_at_utc')}`",
        "",
        "## Runtime Readiness",
        "| Item | Value |",
        "| --- | --- |",
        f"| helios_ready | `{readiness.get('helios_ready')}` |",
        f"| helios_docker_ready | `{readiness.get('helios_docker_ready')}` |",
        f"| awsim_ready | `{readiness.get('awsim_ready')}` |",
        f"| awsim_host_compatible | `{readiness.get('awsim_host_compatible')}` |",
        f"| carla_ready | `{readiness.get('carla_ready')}` |",
        f"| carla_host_compatible | `{readiness.get('carla_host_compatible')}` |",
        f"| carla_docker_ready | `{readiness.get('carla_docker_ready')}` |",
        "",
        "## Probe Readiness",
        "| Probe | Value |",
        "| --- | --- |",
        f"| helios_docker_demo_ready | `{probe_readiness.get('helios_docker_demo_ready')}` |",
        f"| linux_handoff_docker_selftest_ready | `{probe_readiness.get('linux_handoff_docker_selftest_ready')}` |",
        f"| backend_workflow_selftest_ready | `{probe_readiness.get('backend_workflow_selftest_ready')}` |",
        f"| backend_workflow_status | `{probe_readiness.get('backend_workflow_status')}` |",
        f"| carla_docker_pull_ready | `{probe_readiness.get('carla_docker_pull_ready')}` |",
        f"| docker_storage_ready | `{probe_readiness.get('docker_storage_ready')}` |",
        f"| docker_storage_status | `{probe_readiness.get('docker_storage_status')}` |",
        f"| backend_package_workflow_selftest_ready | `{probe_readiness.get('backend_package_workflow_selftest_ready')}` |",
        f"| backend_package_workflow_status | `{probe_readiness.get('backend_package_workflow_status')}` |",
        "",
        "## Workflow Paths",
        "| Path | Value |",
        "| --- | --- |",
        f"| helios_docker_available | `{workflow_paths.get('helios_docker_available')}` |",
        f"| carla_docker_available | `{workflow_paths.get('carla_docker_available')}` |",
        f"| linux_handoff_docker_path_ready | `{workflow_paths.get('linux_handoff_docker_path_ready')}` |",
        f"| backend_workflow_path_ready | `{workflow_paths.get('backend_workflow_path_ready')}` |",
        f"| package_workflow_path_ready | `{workflow_paths.get('package_workflow_path_ready')}` |",
        f"| local_backend_smoke_ready | `{workflow_paths.get('local_backend_smoke_ready')}` |",
        "",
        "## Runtime Strategy",
        "| Backend | Strategy | Preferred Source | Recommended Command | Reason Codes |",
        "| --- | --- | --- | --- | --- |",
    ]
    for backend in ("awsim", "carla"):
        strategy_entry = runtime_strategy.get(backend, {}) if isinstance(runtime_strategy, dict) else {}
        reason_codes = strategy_entry.get("reason_codes", [])
        reason_text = ", ".join(str(item) for item in reason_codes) if isinstance(reason_codes, list) else "-"
        lines.append(
            "| {backend} | `{strategy}` | `{source}` | `{recommended}` | `{reasons}` |".format(
                backend=backend,
                strategy=strategy_entry.get("strategy"),
                source=strategy_entry.get("preferred_runtime_source"),
                recommended=strategy_entry.get("recommended_command"),
                reasons=reason_text,
            )
        )
    lines.extend(
        [
            "",
        "## Selection",
        "| Key | Value |",
        "| --- | --- |",
        ]
    )
    for key in (
        "HELIOS_BIN",
        "HELIOS_DOCKER_IMAGE",
        "HELIOS_DOCKER_BINARY",
        "AWSIM_BIN",
        "AWSIM_RENDERER_MAP",
        "CARLA_BIN",
        "CARLA_DOCKER_IMAGE",
        "CARLA_RENDERER_MAP",
    ):
        lines.append(f"| {key} | `{selection.get(key)}` |")
    if isinstance(issues, list) and issues:
        lines.extend(["", "## Issues"])
        for issue in issues:
            lines.append(f"- `{issue}`")
    if isinstance(commands, dict) and commands:
        lines.extend(["", "## Commands"])
        for key in sorted(commands):
            lines.append(f"- `{key}`: `{commands[key]}`")
    lines.append("")
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
    probes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    host = _host_platform_summary()
    system = host["system"]
    helios = backends["helios"]
    awsim = backends["awsim"]
    carla = backends["carla"]
    helios_docker = runtimes["helios_docker"]
    carla_docker = runtimes["carla_docker"]
    docker_storage_probe = probes.get("docker_storage", {}) if isinstance(probes, dict) else {}
    docker_storage_status, docker_storage_message = _classify_docker_storage_probe(
        docker_storage_probe if isinstance(docker_storage_probe, dict) else None
    )
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
            "docker_runtime_available"
            if carla_docker.get("ready")
            else "runtime_incompatible_host"
            if carla.get("ready") and not carla.get("host_compatible_ready")
            else "runtime_available"
            if carla.get("ready")
            else "source_only"
            if carla.get("source_only")
            else "missing_runtime"
        ),
        "platform_supported": system in {"Linux", "Windows"},
        "platform_note": (
            "CARLA packaged runtime docs target Ubuntu 22.04 or Windows 11. Docker image can still be validated locally if available."
            if system not in {"Linux", "Windows"}
            else "CARLA UE5 docs target Ubuntu 22.04 or Windows 11."
        ),
        "download_options": [
            {
                "name": "CARLA_UE5_Latest.tar.gz",
                "url": "https://s3.us-east-005.backblazeb2.com/carla-releases/Linux/Dev/CARLA_UE5_Latest.tar.gz",
            },
            {
                "name": "CARLA Docker image",
                "command": "docker pull --platform linux/amd64 carlasim/carla:0.10.0",
            },
        ],
        "next_actions": [
            "Use a packaged CARLA release or the CARLA Docker image on a supported Linux/Windows runner.",
            f"Run python3 scripts/acquire_renderer_backend_package.py --backend carla --setup-summary {shlex.quote(str((repo_root / 'artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json').resolve()))} to download and stage the package automatically.",
            "If the Docker image is available locally, validate it with docker image inspect or a lightweight docker run probe.",
            "Export CARLA_BIN to CarlaUnreal.sh or the packaged launcher path when using packaged runtime discovery.",
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
        "docker": {
            "image": carla_docker.get("image"),
            "platform": carla_docker.get("platform"),
            "ready": carla_docker.get("ready"),
            "message": carla_docker.get("image_message") or carla_docker.get("daemon_message"),
        },
    }

    return {
        "host_platform": host,
        "docker": {
            "storage_probe_status": docker_storage_status,
            "storage_probe_message": docker_storage_message,
            "next_actions": [
                "Restart Docker Desktop and rerun the local setup probes.",
                "If the storage probe still reports image/content store corruption, repair Docker Desktop image storage before relying on Docker-based CARLA or HELIOS validation.",
                "Use packaged runtime paths or Linux handoff for backend validation while local Docker storage remains unhealthy.",
            ],
        },
        "helios": helios_hints,
        "awsim": awsim_hints,
        "carla": carla_hints,
    }


def _classify_backend_runtime_strategy(
    *,
    backend: str,
    selection: dict[str, Any],
    readiness: dict[str, Any],
    acquisition_hints: dict[str, Any],
    commands: dict[str, Any],
    summary_path: Path | None = None,
) -> dict[str, Any]:
    backend_hints = acquisition_hints.get(backend, {}) if isinstance(acquisition_hints, dict) else {}
    docker_hints = acquisition_hints.get("docker", {}) if isinstance(acquisition_hints, dict) else {}
    local_download_candidates = backend_hints.get("local_download_candidates", [])
    docker_storage_status = docker_hints.get("storage_probe_status")
    recommended_handoff_command = (
        "python3 scripts/run_renderer_backend_workflow.py "
        f"--backend {backend}"
        + (
            f" --setup-summary {shlex.quote(str(summary_path))}"
            if isinstance(summary_path, Path)
            else ""
        )
        + " --dry-run"
    )
    strategy = "packaged_runtime_required"
    preferred_runtime_source = "packaged"
    recommended_command = commands.get(f"{backend}_acquire")
    reason_codes: list[str] = []
    selected_path = None
    host_compatible = None
    docker_ready = None
    docker_image = None
    if backend == "awsim":
        selected_path = selection.get("AWSIM_BIN")
        local_ready = bool(readiness.get("awsim_ready")) or bool(selected_path)
        host_compatible = bool(readiness.get("awsim_host_compatible"))
        if local_ready and host_compatible:
            strategy = "local_packaged_runtime"
            preferred_runtime_source = "packaged"
            recommended_command = commands.get("awsim_smoke_binary") or commands.get("awsim_smoke")
        elif local_ready and not host_compatible:
            strategy = "linux_handoff_packaged_runtime"
            preferred_runtime_source = "linux_handoff_packaged"
            recommended_command = recommended_handoff_command
            reason_codes.append("HOST_INCOMPATIBLE_PACKAGED_RUNTIME")
        else:
            strategy = "packaged_runtime_required"
            preferred_runtime_source = "packaged"
            recommended_command = commands.get("awsim_acquire")
            reason_codes.append("LOCAL_RUNTIME_MISSING")
    elif backend == "carla":
        selected_path = selection.get("CARLA_BIN")
        local_ready = bool(readiness.get("carla_ready")) or bool(selected_path)
        host_compatible = bool(readiness.get("carla_host_compatible"))
        docker_ready = bool(readiness.get("carla_docker_ready"))
        docker_image = selection.get("CARLA_DOCKER_IMAGE")
        if docker_ready:
            strategy = "local_docker_runtime"
            preferred_runtime_source = "docker"
            recommended_command = commands.get("carla_smoke_docker") or commands.get("carla_smoke")
            reason_codes.append("DOCKER_RUNTIME_AVAILABLE")
        elif local_ready and host_compatible:
            strategy = "local_packaged_runtime"
            preferred_runtime_source = "packaged"
            recommended_command = commands.get("carla_smoke_binary") or commands.get("carla_smoke")
        elif local_ready and not host_compatible:
            strategy = "linux_handoff_packaged_runtime"
            preferred_runtime_source = "linux_handoff_packaged"
            recommended_command = recommended_handoff_command
            reason_codes.append("HOST_INCOMPATIBLE_PACKAGED_RUNTIME")
        elif docker_storage_status in {"image_store_corrupt", "content_store_corrupt", "storage_io_error"}:
            strategy = "packaged_runtime_required"
            preferred_runtime_source = "packaged"
            recommended_command = commands.get("carla_acquire")
            reason_codes.extend(["LOCAL_RUNTIME_MISSING", "DOCKER_STORAGE_CORRUPT"])
        else:
            strategy = "docker_or_packaged_runtime_required"
            preferred_runtime_source = "docker_or_packaged"
            recommended_command = commands.get("carla_docker_pull") or commands.get("carla_acquire")
            reason_codes.append("LOCAL_RUNTIME_MISSING")
            if not docker_ready:
                reason_codes.append("DOCKER_IMAGE_MISSING")
    if backend_hints.get("platform_supported") is False:
        reason_codes.append("PLATFORM_UNSUPPORTED_LOCAL_HOST")
    return {
        "backend": backend,
        "strategy": strategy,
        "preferred_runtime_source": preferred_runtime_source,
        "selected_path": selected_path,
        "host_compatible": host_compatible,
        "docker_ready": docker_ready,
        "docker_image": docker_image,
        "docker_storage_status": docker_storage_status,
        "local_download_candidate_count": len(local_download_candidates) if isinstance(local_download_candidates, list) else 0,
        "reason_codes": sorted(set(reason_codes)),
        "recommended_command": recommended_command,
        "platform_supported": backend_hints.get("platform_supported"),
        "platform_note": backend_hints.get("platform_note"),
    }


def _build_runtime_strategy(summary: dict[str, Any], *, summary_path: Path | None = None) -> dict[str, Any]:
    selection = summary.get("selection", {})
    readiness = summary.get("readiness", {})
    acquisition_hints = summary.get("acquisition_hints", {})
    commands = summary.get("commands", {})
    resolved_summary_path = summary_path
    if resolved_summary_path is None:
        artifacts = summary.get("artifacts", {})
        raw_summary_path = artifacts.get("summary_path") if isinstance(artifacts, dict) else None
        if raw_summary_path:
            resolved_summary_path = Path(str(raw_summary_path))
    return {
        "awsim": _classify_backend_runtime_strategy(
            backend="awsim",
            selection=selection,
            readiness=readiness,
            acquisition_hints=acquisition_hints,
            commands=commands,
            summary_path=resolved_summary_path,
        ),
        "carla": _classify_backend_runtime_strategy(
            backend="carla",
            selection=selection,
            readiness=readiness,
            acquisition_hints=acquisition_hints,
            commands=commands,
            summary_path=resolved_summary_path,
        ),
    }


def build_renderer_backend_local_setup(
    *,
    repo_root: Path,
    search_roots: list[Path] | None = None,
    output_dir: Path | None = None,
    include_default_search_roots: bool = True,
    probe_helios_docker_demo: bool = False,
    helios_docker_probe_config: Path | None = None,
    probe_linux_handoff_docker_selftest: bool = False,
    probe_linux_handoff_docker_selftest_execute: bool = False,
    probe_backend_workflow_selftest: bool = False,
    workflow_selftest_backend: str = "awsim",
    probe_backend_workflow_selftest_execute: bool = False,
    probe_carla_docker_pull: bool = False,
    probe_docker_storage: bool = False,
    probe_backend_package_workflow_selftest: bool = False,
    package_workflow_selftest_backend: str = "awsim",
    package_workflow_selftest_archive_source: str = "local_candidate",
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
    carla_docker = _inspect_carla_docker_runtime()
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
        "CARLA_DOCKER_IMAGE": carla_docker["image"],
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
        "carla_docker_ready": bool(carla_docker.get("ready")),
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
    readiness["carla_local_runtime_ready"] = readiness["carla_ready"] or readiness["carla_docker_ready"]

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
    if not readiness["carla_ready"] and not readiness["carla_docker_ready"]:
        issues.append("CARLA runtime binary is not resolved.")
        if not carla_docker["daemon_ready"]:
            issues.append(f"CARLA docker runtime unavailable: {carla_docker['daemon_message']}")
        elif not carla_docker["image_ready"]:
            issues.append(f"CARLA docker image unavailable: {carla_docker['image_message']}")
    elif readiness["carla_ready"] and not readiness["carla_host_compatible"] and not readiness["carla_docker_ready"]:
        issues.append("CARLA runtime binary is resolved but incompatible with the current host.")

    output_root = (
        _resolve_runtime_path(output_dir)
        if output_dir is not None
        else (repo_root / "artifacts" / "renderer_backend_local_setup").resolve()
    )
    env_path = output_root / "renderer_backend_local.env.sh"
    summary_path = output_root / "renderer_backend_local_setup.json"
    probe_path = output_root / "helios_docker_probe.json"
    handoff_selftest_output_root = output_root / "linux_handoff_docker_selftest_probe"
    handoff_selftest_summary_path = (
        handoff_selftest_output_root / "renderer_backend_linux_handoff_selftest.json"
    )
    workflow_selftest_output_root = output_root / "backend_workflow_selftest_probe"
    workflow_selftest_summary_path = (
        workflow_selftest_output_root / "renderer_backend_workflow_selftest.json"
    )
    carla_docker_pull_summary_path = output_root / "carla_docker_pull_probe.json"
    docker_storage_probe_summary_path = output_root / "docker_storage_probe.json"
    package_workflow_selftest_output_root = output_root / "backend_package_workflow_selftest_probe"
    package_workflow_selftest_summary_path = (
        package_workflow_selftest_output_root / "renderer_backend_package_workflow_selftest.json"
    )
    commands = {
        "helios_docker_demo": "python3 scripts/discover_renderer_backend_local_env.py --probe-helios-docker-demo",
        "linux_handoff_docker_selftest": (
            "python3 scripts/discover_renderer_backend_local_env.py "
            "--probe-linux-handoff-docker-selftest"
        ),
        "backend_workflow_selftest": (
            "python3 scripts/discover_renderer_backend_local_env.py "
            f"--probe-backend-workflow-selftest --workflow-selftest-backend {workflow_selftest_backend}"
        ),
        "carla_docker_pull_probe": (
            "python3 scripts/discover_renderer_backend_local_env.py "
            "--probe-carla-docker-pull"
        ),
        "docker_storage_probe": (
            "python3 scripts/discover_renderer_backend_local_env.py "
            "--probe-docker-storage"
        ),
        "backend_package_workflow_selftest": (
            "python3 scripts/discover_renderer_backend_local_env.py "
            "--probe-backend-package-workflow-selftest "
            f"--package-workflow-selftest-backend {package_workflow_selftest_backend} "
            f"--package-workflow-selftest-archive-source {package_workflow_selftest_archive_source}"
        ),
        "awsim_acquire": (
            "python3 scripts/acquire_renderer_backend_package.py "
            f"--backend awsim --setup-summary {shlex.quote(str(summary_path))}"
        ),
        "carla_acquire": (
            "python3 scripts/acquire_renderer_backend_package.py "
            f"--backend carla --setup-summary {shlex.quote(str(summary_path))}"
        ),
        "carla_docker_pull": "docker pull --platform linux/amd64 carlasim/carla:0.10.0",
        "carla_docker_verify": (
            "docker image inspect carlasim/carla:0.10.0 >/dev/null && "
            "echo 'CARLA docker image is available locally.'"
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
    if probe_linux_handoff_docker_selftest:
        try:
            probe_summary = run_renderer_backend_linux_handoff_selftest(
                repo_root=repo_root,
                output_root=handoff_selftest_output_root,
                summary_path=handoff_selftest_summary_path,
                execute=probe_linux_handoff_docker_selftest_execute,
            )
        except Exception as exc:
            probe_summary = {
                "generated_at_utc": _format_utc(_utc_now()),
                "repo_root": str(repo_root),
                "output_root": str(handoff_selftest_output_root),
                "summary_path": str(handoff_selftest_summary_path),
                "execute": probe_linux_handoff_docker_selftest_execute,
                "success": False,
                "error": str(exc),
            }
            _write_json(handoff_selftest_summary_path, probe_summary)
        probes["linux_handoff_docker_selftest"] = probe_summary
        if not probe_summary.get("success", False):
            issues.append("Linux handoff Docker self-test failed.")
    if probe_backend_workflow_selftest:
        try:
            from hybrid_sensor_sim.tools.renderer_backend_workflow_selftest import (
                run_renderer_backend_workflow_selftest,
            )

            probe_summary = run_renderer_backend_workflow_selftest(
                backend=workflow_selftest_backend,
                repo_root=repo_root,
                output_root=workflow_selftest_output_root,
                summary_path=workflow_selftest_summary_path,
                docker_handoff_execute=probe_backend_workflow_selftest_execute,
            )
        except Exception as exc:
            probe_summary = {
                "generated_at_utc": _format_utc(_utc_now()),
                "backend": workflow_selftest_backend,
                "repo_root": str(repo_root),
                "output_root": str(workflow_selftest_output_root),
                "summary_path": str(workflow_selftest_summary_path),
                "docker_handoff_execute": probe_backend_workflow_selftest_execute,
                "success": False,
                "error": str(exc),
            }
            _write_json(workflow_selftest_summary_path, probe_summary)
        probes["backend_workflow_selftest"] = probe_summary
        if not probe_summary.get("success", False):
            issues.append("Backend workflow self-test failed.")
    if probe_carla_docker_pull:
        probe_summary = _run_carla_docker_pull_probe(
            image=str(carla_docker.get("image") or _DEFAULT_CARLA_DOCKER_IMAGE),
            platform_name=str(carla_docker.get("platform") or _DEFAULT_CARLA_DOCKER_PLATFORM),
        )
        _write_json(carla_docker_pull_summary_path, probe_summary)
        probes["carla_docker_pull"] = probe_summary
        if not probe_summary.get("success", False):
            stderr = str(probe_summary.get("stderr") or "").strip()
            if stderr:
                issues.append(f"CARLA docker pull probe failed: {stderr}")
            else:
                issues.append("CARLA docker pull probe failed.")
    if probe_docker_storage:
        probe_summary = _run_docker_storage_probe()
        _write_json(docker_storage_probe_summary_path, probe_summary)
        probes["docker_storage"] = probe_summary
        if not probe_summary.get("success", False):
            stderr = str(probe_summary.get("stderr") or "").strip()
            if stderr:
                issues.append(f"Docker storage probe failed: {stderr}")
            else:
                issues.append("Docker storage probe failed.")
    if probe_backend_package_workflow_selftest:
        try:
            from hybrid_sensor_sim.tools.renderer_backend_package_workflow_selftest import (
                run_renderer_backend_package_workflow_selftest,
            )

            probe_summary = run_renderer_backend_package_workflow_selftest(
                backend=package_workflow_selftest_backend,
                output_root=package_workflow_selftest_output_root,
                summary_path=package_workflow_selftest_summary_path,
                archive_source=package_workflow_selftest_archive_source,
            )
        except Exception as exc:
            probe_summary = {
                "generated_at_utc": _format_utc(_utc_now()),
                "backend": package_workflow_selftest_backend,
                "output_root": str(package_workflow_selftest_output_root),
                "summary_path": str(package_workflow_selftest_summary_path),
                "archive_source": package_workflow_selftest_archive_source,
                "success": False,
                "error": str(exc),
            }
            _write_json(package_workflow_selftest_summary_path, probe_summary)
        probes["backend_package_workflow_selftest"] = probe_summary
        if not probe_summary.get("success", False):
            issues.append("Backend package workflow self-test failed.")
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
            "carla_docker": carla_docker,
        },
        readiness=readiness,
        probes=probes,
    )
    summary = {
        "generated_at_utc": _format_utc(_utc_now()),
        "search_roots": [str(path) for path in all_search_roots],
        "backends": {
            "helios": helios,
            "awsim": awsim,
            "carla": carla,
        },
        "runtimes": {
            "helios_docker": helios_docker,
            "carla_docker": carla_docker,
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
            "report_path": str(output_root / "renderer_backend_local_report.md"),
            "helios_docker_probe_path": str(probe_path),
            "linux_handoff_docker_selftest_probe_path": str(handoff_selftest_summary_path),
            "backend_workflow_selftest_probe_path": str(workflow_selftest_summary_path),
            "carla_docker_pull_probe_path": str(carla_docker_pull_summary_path),
            "docker_storage_probe_path": str(docker_storage_probe_summary_path),
            "backend_package_workflow_selftest_probe_path": str(package_workflow_selftest_summary_path),
        },
    }
    summary["probe_readiness"] = _build_probe_readiness(summary)
    summary["workflow_paths"] = _build_workflow_paths(readiness, summary["probe_readiness"])
    summary["runtime_strategy"] = _build_runtime_strategy(summary, summary_path=summary_path)
    return summary


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
        probe_linux_handoff_docker_selftest=args.probe_linux_handoff_docker_selftest,
        probe_linux_handoff_docker_selftest_execute=args.probe_linux_handoff_docker_selftest_execute,
        probe_backend_workflow_selftest=args.probe_backend_workflow_selftest,
        workflow_selftest_backend=args.workflow_selftest_backend,
        probe_backend_workflow_selftest_execute=args.probe_backend_workflow_selftest_execute,
        probe_carla_docker_pull=args.probe_carla_docker_pull,
        probe_docker_storage=args.probe_docker_storage,
        probe_backend_package_workflow_selftest=args.probe_backend_package_workflow_selftest,
        package_workflow_selftest_backend=args.package_workflow_selftest_backend,
        package_workflow_selftest_archive_source=args.package_workflow_selftest_archive_source,
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
    report_path = Path(summary["artifacts"]["report_path"])
    summary["artifacts"]["summary_path"] = str(summary_path)
    summary["artifacts"]["env_path"] = str(env_path)
    _write_json(summary_path, summary)
    _write_text(env_path, _render_env_file(summary))
    _write_text(report_path, _render_local_setup_report(summary, summary_path))
    print(json.dumps(summary, indent=2))
    return 0 if summary["readiness"]["awsim_smoke_ready"] or summary["readiness"]["carla_smoke_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
