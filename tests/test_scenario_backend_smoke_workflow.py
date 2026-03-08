from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.scenario_backend_smoke_workflow import (
    SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
    run_scenario_backend_smoke_workflow,
)
from hybrid_sensor_sim.tools.scenario_batch_workflow import run_scenario_batch_workflow
from hybrid_sensor_sim.tools.scenario_runtime_bridge import (
    SCENARIO_RUNTIME_BRIDGE_SCHEMA_VERSION_V0,
    build_smoke_ready_scenario,
)
from hybrid_sensor_sim.tools.scenario_variant_workflow import run_scenario_variant_workflow


P_VALIDATION_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"
P_SIM_ENGINE_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"
P_MAP_TOOLSET_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_map_toolset"


def _write_fake_helios_script(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    out="$2"
    shift 2
  else
    shift
  fi
done
mkdir -p "${out}/demo/2026-01-01_00-00-00"
rootdir="${out}/demo/2026-01-01_00-00-00"
echo "Output directory: \\"${rootdir}\\""
cat > "${rootdir}/scan_points.xyz" <<EOF
10.0 0.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
1.0 0.0 0.0 1.0 0.0 0.0 0.0
EOF
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_fake_backend_success(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["BACKEND_OUTPUT_SPEC_PATH"]).read_text(encoding="utf-8"))
for entry in spec.get("expected_outputs", []):
    path = Path(entry["path"])
    if entry.get("kind") == "directory":
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"artifact_key": entry["artifact_key"]}), encoding="utf-8")
PY
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_smoke_base_config(
    *,
    root: Path,
    helios_bin: Path,
    output_dir: Path,
) -> Path:
    config_path = root / "renderer_backend_smoke_base.json"
    config_path.write_text(
        json.dumps(
            {
                "mode": "hybrid_auto",
                "helios_runtime": "binary",
                "helios_bin": str(helios_bin),
                "scenario_path": str((root / "placeholder_scene.json").resolve()),
                "output_dir": str(output_dir.resolve()),
                "sensor_profile": "smoke",
                "seed": 7,
                "options": {
                    "helios_runtime": "binary",
                    "execute_helios": True,
                    "survey_generate_from_scenario": True,
                    "survey_generated_name": "scenario_backend_smoke_test",
                    "survey_scene_ref": "data/scenes/demo/plane_scene.xml#plane_scene",
                    "survey_platform_ref": "data/platforms.xml#tripod",
                    "survey_scanner_ref": "data/scanners_tls.xml#panoscanner",
                    "survey_scanner_settings_id": "scaset",
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": False,
                    "renderer_backend": "none",
                    "renderer_execute": False,
                    "renderer_fail_on_error": False,
                    "renderer_command": [],
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path


class ScenarioBackendSmokeWorkflowTests(unittest.TestCase):
    def test_build_smoke_ready_scenario_from_scenario_definition(self) -> None:
        scenario_path = P_SIM_ENGINE_FIXTURE_ROOT / "highway_map_route_following_v0.json"
        smoke_scenario, bridge_manifest = build_smoke_ready_scenario(
            source_payload_path=scenario_path,
            source_payload_kind="scenario_definition_v0",
            lane_spacing_m=3.5,
        )
        self.assertEqual(bridge_manifest["scenario_runtime_bridge_schema_version"], SCENARIO_RUNTIME_BRIDGE_SCHEMA_VERSION_V0)
        self.assertEqual(smoke_scenario["name"], "SC_HWY_MAP_ROUTE_001")
        self.assertEqual(smoke_scenario["objects"][0]["id"], "ego")
        self.assertAlmostEqual(smoke_scenario["objects"][0]["pose"][1], 0.0)
        self.assertEqual(len(smoke_scenario["objects"]), 3)
        self.assertEqual(bridge_manifest["object_count"], 3)
        self.assertEqual(len(smoke_scenario["ego_trajectory"]), 2)

    def test_run_scenario_backend_smoke_workflow_runs_smoke_from_variant_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            variant_result = run_scenario_variant_workflow(
                logical_scenarios_path="",
                scenario_language_profile="highway_mixed_payloads_v0",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                out_root=root / "variant_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_success.sh"
            _write_fake_backend_success(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            result = run_scenario_backend_smoke_workflow(
                variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                batch_workflow_report_path="",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "backend_smoke_workflow",
                selection_strategy="variant_id",
                selected_variant_id="scn_direct_object_sim_0001",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path="",
                backend_workflow_summary_path="",
                backend_bin=str(fake_backend),
                renderer_map="Town07",
                option_overrides=[],
                skip_smoke=False,
            )

            workflow_report = result["workflow_report"]
            self.assertEqual(
                workflow_report["scenario_backend_smoke_workflow_report_schema_version"],
                SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(workflow_report["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(workflow_report["selection"]["variant_id"], "scn_direct_object_sim_0001")
            self.assertEqual(workflow_report["selection"]["bridge_source_origin"], "rendered_payload_path")
            smoke_summary = workflow_report["smoke"]["summary"]
            self.assertEqual(smoke_summary["backend"], "awsim")
            self.assertTrue(smoke_summary["success"])
            self.assertEqual(smoke_summary["output_comparison_status"], "MATCHED")
            smoke_scenario = json.loads(
                Path(workflow_report["artifacts"]["smoke_scenario_path"]).read_text(encoding="utf-8")
            )
            self.assertEqual(smoke_scenario["objects"][0]["id"], "ego")
            self.assertTrue(Path(workflow_report["smoke"]["summary_path"]).is_file())

    def test_run_scenario_backend_smoke_workflow_selects_worst_logical_scenario_from_batch_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch_result = run_scenario_batch_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            result = run_scenario_backend_smoke_workflow(
                variant_workflow_report_path="",
                batch_workflow_report_path=str(batch_result["workflow_report_path"]),
                smoke_config_path=smoke_config,
                backend="carla",
                out_root=root / "backend_smoke_workflow",
                selection_strategy="worst_logical_scenario",
                selected_variant_id="",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path="",
                backend_workflow_summary_path="",
                backend_bin="",
                renderer_map="Town03",
                option_overrides=[],
                skip_smoke=True,
            )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "BRIDGED_ONLY")
            self.assertEqual(
                workflow_report["selection"]["logical_scenario_id"],
                batch_result["workflow_report"]["status_summary"]["worst_logical_scenario_row"]["logical_scenario_id"],
            )
            self.assertTrue(Path(workflow_report["artifacts"]["smoke_input_config_path"]).is_file())
            self.assertEqual(workflow_report["smoke"]["requested"], False)

    def test_run_scenario_backend_smoke_workflow_resolves_backend_from_setup_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            variant_result = run_scenario_variant_workflow(
                logical_scenarios_path="",
                scenario_language_profile="highway_mixed_payloads_v0",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                out_root=root / "variant_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_success.sh"
            _write_fake_backend_success(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "AWSIM_BIN": str(fake_backend.resolve()),
                            "AWSIM_RENDERER_MAP": "Town12",
                        }
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            result = run_scenario_backend_smoke_workflow(
                variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                batch_workflow_report_path="",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "backend_smoke_workflow",
                selection_strategy="variant_id",
                selected_variant_id="scn_direct_object_sim_0001",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path=str(setup_summary),
                backend_workflow_summary_path="",
                backend_bin="",
                renderer_map="",
                option_overrides=[],
                skip_smoke=False,
            )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(
                workflow_report["runtime_selection"]["backend_bin_source"],
                "setup_summary",
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["renderer_map_source"],
                "setup_summary",
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["renderer_map"],
                "Town12",
            )

    def test_scenario_backend_smoke_workflow_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_backend_smoke_workflow.py"
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("Select a scenario variant", completed.stdout)
