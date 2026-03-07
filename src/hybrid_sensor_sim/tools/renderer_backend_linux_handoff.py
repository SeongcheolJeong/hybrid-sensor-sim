from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.renderer_backend_workflow import (
    _load_json,
    _resolve_path,
    _verify_linux_handoff_bundle,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unpack, verify, and optionally execute a Linux handoff bundle produced by renderer_backend_workflow.py."
    )
    parser.add_argument(
        "--bundle",
        type=Path,
        required=True,
        help="Path to renderer_backend_workflow_linux_handoff_bundle.tar.gz.",
    )
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
        help="Linux runner repo checkout root. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--extract-root",
        type=Path,
        help="Directory where the bundle will be unpacked. Defaults under output-root.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/renderer_backend_linux_handoff_run"),
        help="Directory where execution/verification artifacts will be written.",
    )
    parser.add_argument(
        "--verification-manifest-path",
        type=Path,
        help="Where to write renderer_backend_linux_handoff_verification.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_linux_handoff_run.json. Defaults under output-root.",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Verify and unpack only; do not execute the extracted handoff script.",
    )
    parser.add_argument(
        "--forward-arg",
        action="append",
        default=[],
        help="Forwarded to the extracted handoff script when execution is enabled.",
    )
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _find_handoff_script_relative_path(transfer_manifest: dict[str, Any]) -> str:
    entries = transfer_manifest.get("entries", [])
    if not isinstance(entries, list):
        raise ValueError("Transfer manifest entries must be a list.")
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("kind") == "handoff_generated_script":
            target_relative_path = entry.get("target_relative_path")
            if isinstance(target_relative_path, str) and target_relative_path.strip():
                return target_relative_path
    raise ValueError("Transfer manifest does not contain a handoff_generated_script entry.")


def run_renderer_backend_linux_handoff(
    *,
    bundle_path: Path,
    transfer_manifest_path: Path,
    bundle_manifest_path: Path | None = None,
    repo_root: Path | None = None,
    extract_root: Path | None = None,
    output_root: Path,
    verification_manifest_path: Path | None = None,
    summary_path: Path | None = None,
    skip_run: bool = False,
    forward_args: list[str] | None = None,
) -> dict[str, Any]:
    output_root = _resolve_path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    bundle_path = _resolve_path(bundle_path)
    transfer_manifest_path = _resolve_path(transfer_manifest_path)
    bundle_manifest_path = (
        _resolve_path(bundle_manifest_path)
        if bundle_manifest_path is not None
        else (output_root / "renderer_backend_linux_handoff_bundle_manifest.json").resolve()
    )
    repo_root = _resolve_path(repo_root) if repo_root is not None else Path.cwd().resolve()
    extract_root = (
        _resolve_path(extract_root)
        if extract_root is not None
        else (output_root / "extracted").resolve()
    )
    verification_manifest_path = (
        _resolve_path(verification_manifest_path)
        if verification_manifest_path is not None
        else (output_root / "renderer_backend_linux_handoff_verification.json").resolve()
    )
    summary_path = (
        _resolve_path(summary_path)
        if summary_path is not None
        else (output_root / "renderer_backend_linux_handoff_run.json").resolve()
    )
    forward_args = list(forward_args or [])

    transfer_manifest = _load_json(transfer_manifest_path)
    verification = _verify_linux_handoff_bundle(
        transfer_manifest=transfer_manifest,
        bundle_path=bundle_path,
        bundle_manifest_path=bundle_manifest_path,
        extract_root=extract_root,
        verification_manifest_path=verification_manifest_path,
    )

    execution: dict[str, Any] = {
        "attempted": False,
        "executed": False,
        "exit_code": None,
        "stdout": "",
        "stderr": "",
        "handoff_script_path": None,
    }
    if verification.get("verified") and not skip_run:
        handoff_script_relative_path = _find_handoff_script_relative_path(transfer_manifest)
        handoff_script_path = extract_root / handoff_script_relative_path
        env = os.environ.copy()
        env["WORKFLOW_REPO_ROOT"] = str(repo_root)
        execution["attempted"] = True
        execution["handoff_script_path"] = str(handoff_script_path)
        proc = subprocess.run(
            ["bash", str(handoff_script_path), *forward_args],
            cwd=repo_root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        execution.update(
            {
                "executed": True,
                "exit_code": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
        )

    summary = {
        "bundle_path": str(bundle_path),
        "transfer_manifest_path": str(transfer_manifest_path),
        "bundle_manifest_path": str(bundle_manifest_path),
        "repo_root": str(repo_root),
        "extract_root": str(extract_root),
        "verification_manifest_path": str(verification_manifest_path),
        "summary_path": str(summary_path),
        "verified": bool(verification.get("verified")),
        "skip_run": skip_run,
        "forward_args": forward_args,
        "verification": verification,
        "execution": execution,
    }
    _write_json(summary_path, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    summary = run_renderer_backend_linux_handoff(
        bundle_path=args.bundle,
        transfer_manifest_path=args.transfer_manifest,
        bundle_manifest_path=args.bundle_manifest,
        repo_root=args.repo_root,
        extract_root=args.extract_root,
        output_root=args.output_root,
        verification_manifest_path=args.verification_manifest_path,
        summary_path=args.summary_path,
        skip_run=args.skip_run,
        forward_args=list(args.forward_arg),
    )
    print(json.dumps(summary, indent=2))
    if not summary["verified"]:
        return 2
    execution = summary.get("execution", {})
    if execution.get("attempted") and execution.get("exit_code") not in (None, 0):
        return int(execution["exit_code"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
