from __future__ import annotations

import argparse
import json
import os
import shlex
from pathlib import Path
from typing import Any


_DEFAULT_BACKEND_MAPS = {
    "awsim": "SampleMap",
    "carla": "Town03",
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
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _resolve_runtime_path(raw: str | Path) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _is_executable_file(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def _path_record(path: Path, *, origin: str, kind: str) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "origin": origin,
        "kind": kind,
        "exists": path.exists(),
        "executable": _is_executable_file(path),
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
        ],
        "carla": [
            repo_root / "third_party/carla/CarlaUE4.sh",
            repo_root / "third_party/carla/CarlaUE4.app/Contents/MacOS/CarlaUE4",
            repo_root / "third_party/carla/CarlaUE5.sh",
            repo_root / "third_party/carla/CarlaUE5.app/Contents/MacOS/CarlaUE5",
        ],
    }


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


def _select_preferred_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for candidate in candidates:
        if candidate.get("exists") and candidate.get("executable"):
            return candidate
    for candidate in candidates:
        if candidate.get("exists"):
            return candidate
    return None


def _discover_backend_candidates(
    *,
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
            f"# awsim_smoke_ready={readiness.get('awsim_smoke_ready', False)}",
            f"# carla_smoke_ready={readiness.get('carla_smoke_ready', False)}",
            "#",
            "# Example:",
            "# source artifacts/renderer_backend_local_setup/renderer_backend_local.env.sh",
            "# python3 scripts/run_renderer_backend_smoke.py --config configs/renderer_backend_smoke.awsim.local.example.json --backend awsim",
            "",
        ]
    )
    return "\n".join(lines)


def build_renderer_backend_local_setup(
    *,
    repo_root: Path,
    search_roots: list[Path] | None = None,
    output_dir: Path | None = None,
    include_default_search_roots: bool = True,
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
    helios = _discover_backend_candidates(
        repo_root=repo_root,
        search_roots=all_search_roots,
        env_var="HELIOS_BIN",
        repo_candidates=repo_candidates["helios"],
        scanned_names={"helios++", "helios"},
        reference_roots=reference_roots["helios"],
    )
    awsim = _discover_backend_candidates(
        repo_root=repo_root,
        search_roots=all_search_roots,
        env_var="AWSIM_BIN",
        repo_candidates=repo_candidates["awsim"],
        scanned_names={"awsim", "awsim.x86_64", "AWSIM.x86_64", "AWSIM"},
        reference_roots=reference_roots["awsim"],
    )
    carla = _discover_backend_candidates(
        repo_root=repo_root,
        search_roots=all_search_roots,
        env_var="CARLA_BIN",
        repo_candidates=repo_candidates["carla"],
        scanned_names={"CarlaUE4.sh", "CarlaUE4", "CarlaUE5.sh", "CarlaUE5"},
        reference_roots=reference_roots["carla"],
    )

    selection = {
        "HELIOS_BIN": helios.get("selected_path"),
        "AWSIM_BIN": awsim.get("selected_path"),
        "AWSIM_RENDERER_MAP": os.getenv("AWSIM_RENDERER_MAP", _DEFAULT_BACKEND_MAPS["awsim"]),
        "CARLA_BIN": carla.get("selected_path"),
        "CARLA_RENDERER_MAP": os.getenv("CARLA_RENDERER_MAP", _DEFAULT_BACKEND_MAPS["carla"]),
    }
    readiness = {
        "helios_ready": bool(helios.get("ready")),
        "awsim_ready": bool(awsim.get("ready")),
        "carla_ready": bool(carla.get("ready")),
    }
    readiness["awsim_smoke_ready"] = readiness["helios_ready"] and readiness["awsim_ready"]
    readiness["carla_smoke_ready"] = readiness["helios_ready"] and readiness["carla_ready"]

    issues: list[str] = []
    if not readiness["helios_ready"]:
        issues.append("HELIOS binary is not resolved.")
    if not readiness["awsim_ready"]:
        issues.append("AWSIM runtime binary is not resolved.")
    if not readiness["carla_ready"]:
        issues.append("CARLA runtime binary is not resolved.")

    output_root = (
        _resolve_runtime_path(output_dir)
        if output_dir is not None
        else (repo_root / "artifacts" / "renderer_backend_local_setup").resolve()
    )
    env_path = output_root / "renderer_backend_local.env.sh"
    summary_path = output_root / "renderer_backend_local_setup.json"
    commands = {
        "awsim_smoke": (
            f"source {shlex.quote(str(env_path))} && "
            "python3 scripts/run_renderer_backend_smoke.py "
            "--config configs/renderer_backend_smoke.awsim.local.example.json --backend awsim"
        ),
        "carla_smoke": (
            f"source {shlex.quote(str(env_path))} && "
            "python3 scripts/run_renderer_backend_smoke.py "
            "--config configs/renderer_backend_smoke.carla.local.example.json --backend carla"
        ),
    }
    return {
        "search_roots": [str(path) for path in all_search_roots],
        "backends": {
            "helios": helios,
            "awsim": awsim,
            "carla": carla,
        },
        "selection": selection,
        "readiness": readiness,
        "issues": issues,
        "commands": commands,
        "artifacts": {
            "summary_path": str(summary_path),
            "env_path": str(env_path),
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
