from __future__ import annotations

import contextlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_linux_handoff import (
    main as linux_handoff_main,
    run_renderer_backend_linux_handoff,
)
from hybrid_sensor_sim.tools.renderer_backend_workflow import _sha256_file


def _write_bundle_file(root: Path, relative_path: str, content: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


def _build_test_bundle(
    *,
    root: Path,
    handoff_script_body: str,
) -> tuple[Path, Path, Path]:
    bundle_root = root / "bundle_src"
    script_path = _write_bundle_file(
        bundle_root,
        "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.sh",
        handoff_script_body,
    )
    _write_bundle_file(
        bundle_root,
        "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json",
        "{}",
    )
    _write_bundle_file(
        bundle_root,
        "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh",
        "#!/usr/bin/env bash\n",
    )
    bundle_path = root / "handoff_bundle.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as handle:
        for path in sorted(bundle_root.rglob("*")):
            handle.add(path, arcname=str(path.relative_to(bundle_root)))
    bundle_manifest_path = root / "bundle_manifest.json"
    bundle_manifest_path.write_text(
        json.dumps(
            {
                "bundle_path": str(bundle_path),
                "bundle_sha256": _sha256_file(bundle_path),
            }
        ),
        encoding="utf-8",
    )
    transfer_manifest_path = root / "transfer_manifest.json"
    transfer_manifest_path.write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "kind": "handoff_generated_script",
                        "local_path": str(script_path),
                        "target_relative_path": "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.sh",
                        "sha256": _sha256_file(script_path),
                        "size_bytes": script_path.stat().st_size,
                    },
                    {
                        "kind": "handoff_generated_config",
                        "local_path": str(bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json"),
                        "target_relative_path": "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json",
                        "sha256": _sha256_file(bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json"),
                        "size_bytes": (bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json").stat().st_size,
                    },
                    {
                        "kind": "handoff_generated_env",
                        "local_path": str(bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh"),
                        "target_relative_path": "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh",
                        "sha256": _sha256_file(bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh"),
                        "size_bytes": (bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh").stat().st_size,
                    },
                ],
                "verifiable_entries": [
                    {
                        "target_relative_path": "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.sh",
                        "sha256": _sha256_file(script_path),
                        "size_bytes": script_path.stat().st_size,
                        "kind": "handoff_generated_script",
                    },
                    {
                        "target_relative_path": "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json",
                        "sha256": _sha256_file(bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json"),
                        "size_bytes": (bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff_config.json").stat().st_size,
                        "kind": "handoff_generated_config",
                    },
                    {
                        "target_relative_path": "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh",
                        "sha256": _sha256_file(bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh"),
                        "size_bytes": (bundle_root / "artifacts/renderer_backend_workflow/awsim/renderer_backend_workflow_linux_handoff.env.sh").stat().st_size,
                        "kind": "handoff_generated_env",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return bundle_path, transfer_manifest_path, bundle_manifest_path


class RendererBackendLinuxHandoffTests(unittest.TestCase):
    def test_linux_handoff_can_verify_without_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_path, transfer_manifest_path, bundle_manifest_path = _build_test_bundle(
                root=root,
                handoff_script_body="#!/usr/bin/env bash\nexit 0\n",
            )

            summary = run_renderer_backend_linux_handoff(
                bundle_path=bundle_path,
                transfer_manifest_path=transfer_manifest_path,
                bundle_manifest_path=bundle_manifest_path,
                output_root=root / "output",
                skip_run=True,
            )

            self.assertTrue(summary["verified"])
            self.assertFalse(summary["execution"]["attempted"])
            self.assertTrue(Path(summary["verification_manifest_path"]).exists())

    def test_linux_handoff_can_execute_extracted_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            marker_path = repo_root / "handoff_ran.txt"
            bundle_path, transfer_manifest_path, bundle_manifest_path = _build_test_bundle(
                root=root,
                handoff_script_body=(
                    "#!/usr/bin/env bash\n"
                    "set -euo pipefail\n"
                    "echo runner-ok > \"$WORKFLOW_REPO_ROOT/handoff_ran.txt\"\n"
                ),
            )

            summary = run_renderer_backend_linux_handoff(
                bundle_path=bundle_path,
                transfer_manifest_path=transfer_manifest_path,
                bundle_manifest_path=bundle_manifest_path,
                repo_root=repo_root,
                output_root=root / "output",
                skip_run=False,
            )

            self.assertTrue(summary["verified"])
            self.assertTrue(summary["execution"]["attempted"])
            self.assertEqual(summary["execution"]["exit_code"], 0)
            self.assertTrue(marker_path.exists())
            self.assertEqual(marker_path.read_text(encoding="utf-8").strip(), "runner-ok")

    def test_linux_handoff_main_returns_two_when_verification_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_path, transfer_manifest_path, bundle_manifest_path = _build_test_bundle(
                root=root,
                handoff_script_body="#!/usr/bin/env bash\nexit 0\n",
            )
            payload = json.loads(bundle_manifest_path.read_text(encoding="utf-8"))
            payload["bundle_sha256"] = "bad"
            bundle_manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = linux_handoff_main(
                    [
                        "--bundle",
                        str(bundle_path),
                        "--transfer-manifest",
                        str(transfer_manifest_path),
                        "--bundle-manifest",
                        str(bundle_manifest_path),
                        "--output-root",
                        str(root / "output"),
                        "--skip-run",
                    ]
                )

            self.assertEqual(exit_code, 2)


if __name__ == "__main__":
    unittest.main()
