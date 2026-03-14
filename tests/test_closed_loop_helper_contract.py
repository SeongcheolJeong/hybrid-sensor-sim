from __future__ import annotations

import json
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.scenario_closed_loop_demo_workflow import _HELPER_SCRIPT_NAMES


REPO_ROOT = Path("/Users/seongcheoljeong/Documents/Test")
TEMPLATE_BIN_ROOT = REPO_ROOT / "examples" / "closed_loop" / "linux_runtime_root" / "bin"
REQUEST_EXAMPLE_PATH = REPO_ROOT / "examples" / "closed_loop" / "closed_loop_demo_request_v0.json"


class ClosedLoopHelperContractTests(unittest.TestCase):
    def test_helper_templates_match_workflow_contract(self) -> None:
        expected_names = sorted(_HELPER_SCRIPT_NAMES.values())
        actual_names = sorted(
            path.name for path in TEMPLATE_BIN_ROOT.iterdir() if path.is_file()
        )
        self.assertEqual(actual_names, expected_names)

    def test_helper_templates_are_executable(self) -> None:
        for helper_path in TEMPLATE_BIN_ROOT.iterdir():
            if not helper_path.is_file():
                continue
            self.assertTrue(helper_path.stat().st_mode & 0o111, str(helper_path))

    def test_request_example_contains_preflight_and_full_demo_requests(self) -> None:
        payload = json.loads(REQUEST_EXAMPLE_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "closed_loop_demo_request_v0")
        for field_name in ("preflight_only_request", "full_demo_request"):
            request = payload[field_name]
            self.assertIn("project_id", request)
            self.assertIn("payload", request)
            self.assertIsInstance(request["payload"], dict)
            for required_payload_key in (
                "scenario_path",
                "linux_runtime_root",
                "autoware_workspace_root",
                "awsim_runtime_root",
                "map_path",
                "route_path",
                "out_root",
            ):
                self.assertIn(required_payload_key, request["payload"])
        self.assertTrue(payload["preflight_only_request"]["payload"]["preflight_only"])
        self.assertFalse(payload["full_demo_request"]["payload"]["preflight_only"])


if __name__ == "__main__":
    unittest.main()
