from __future__ import annotations

import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.scenario_batch_gate_catalog import (
    build_scenario_batch_gate_profile_catalog,
    default_scenario_batch_gate_profile_dir,
    resolve_scenario_batch_gate_profile_path,
)


P_VALIDATION_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"


class ScenarioBatchGateCatalogTests(unittest.TestCase):
    def test_build_gate_profile_catalog_filters_non_gate_json_files(self) -> None:
        catalog = build_scenario_batch_gate_profile_catalog(P_VALIDATION_FIXTURE_ROOT)
        self.assertIn("scenario_batch_gate_strict_v0", catalog)
        self.assertTrue(catalog["scenario_batch_gate_strict_v0"]["path"].endswith("scenario_batch_gate_strict_v0.json"))
        self.assertNotIn("highway_mixed_payloads_v0", catalog)

    def test_resolve_gate_profile_path_uses_catalog_id(self) -> None:
        path = resolve_scenario_batch_gate_profile_path(
            gate_profile="",
            gate_profile_id="scenario_batch_gate_strict_v0",
            gate_profile_dir=str(P_VALIDATION_FIXTURE_ROOT),
        )
        self.assertEqual(path, (P_VALIDATION_FIXTURE_ROOT / "scenario_batch_gate_strict_v0.json").resolve())

    def test_resolve_gate_profile_path_rejects_unknown_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown scenario batch gate profile id"):
            resolve_scenario_batch_gate_profile_path(
                gate_profile="",
                gate_profile_id="missing_profile",
                gate_profile_dir=str(P_VALIDATION_FIXTURE_ROOT),
            )

    def test_resolve_gate_profile_path_rejects_conflicting_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "use either --gate-profile or --gate-profile-id"):
            resolve_scenario_batch_gate_profile_path(
                gate_profile=str(P_VALIDATION_FIXTURE_ROOT / "scenario_batch_gate_strict_v0.json"),
                gate_profile_id="scenario_batch_gate_strict_v0",
                gate_profile_dir=str(P_VALIDATION_FIXTURE_ROOT),
            )

    def test_default_gate_profile_dir_exists(self) -> None:
        self.assertTrue(default_scenario_batch_gate_profile_dir().is_dir())


if __name__ == "__main__":
    unittest.main()
