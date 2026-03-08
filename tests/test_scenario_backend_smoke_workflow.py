from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
)
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


def _write_fake_backend_unexpected(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["BACKEND_OUTPUT_SPEC_PATH"]).read_text(encoding="utf-8"))
output_root = Path(os.environ["BACKEND_OUTPUT_ROOT"])
for entry in spec.get("expected_outputs", []):
    path = Path(entry["path"])
    if entry.get("kind") == "directory":
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"artifact_key": entry["artifact_key"]}), encoding="utf-8")
(output_root / "extras").mkdir(parents=True, exist_ok=True)
(output_root / "extras" / "unexpected.log").write_text("unexpected\n", encoding="utf-8")
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


def _init_guard_repo(repo_root: Path) -> None:
    subprocess.run(["git", "init", "-b", "main"], cwd=repo_root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repo_root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repo_root,
        check=True,
    )
    (repo_root / "src").mkdir(parents=True, exist_ok=True)
    (repo_root / "src" / "tracked_module.py").write_text("value = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo_root, check=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=repo_root, check=True)
    subprocess.run(
        ["git", "update-ref", "refs/remotes/origin/main", "HEAD"],
        cwd=repo_root,
        check=True,
    )


def _write_guard_metadata(metadata_root: Path) -> None:
    metadata_root.mkdir(parents=True, exist_ok=True)
    traceability_payload = {
        "schema_version": AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
        "generated_at_utc": "2026-03-08T00:00:00Z",
        "paths": [
            {
                "current_path": "src/tracked_module.py",
                "path_kind": "library",
                "block_ids": ["p_sim_engine.object_sim_core"],
                "project_ids": ["P_Sim-Engine"],
                "result_role": "core_logic",
                "current_intro_commit": "1111111",
                "current_latest_touch_commit": "2222222",
            }
        ],
    }
    (metadata_root / "result_traceability_index_v0.json").write_text(
        json.dumps(traceability_payload, indent=2) + "\n",
        encoding="utf-8",
    )


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
                selection_strategy="first_successful_variant",
                selected_variant_id="",
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
            self.assertTrue(workflow_report["selection"]["variant_id"])
            self.assertIn(
                workflow_report["selection"]["bridge_source_origin"],
                {"rendered_payload_path", "replay_scenario_path"},
            )
            smoke_summary = workflow_report["smoke"]["summary"]
            self.assertEqual(smoke_summary["backend"], "awsim")
            self.assertTrue(smoke_summary["success"])
            self.assertEqual(smoke_summary["output_comparison_status"], "MATCHED")
            self.assertEqual(smoke_summary["output_origin_status"], "BACKEND_RUNTIME_ONLY")
            self.assertIn(workflow_report["autoware"]["status"], {"READY", "DEGRADED"})
            self.assertEqual(workflow_report["autoware"]["availability_mode"], "runtime")
            self.assertTrue(workflow_report["autoware"]["dataset_ready"])
            self.assertTrue(workflow_report["autoware"]["consumer_ready"])
            self.assertEqual(workflow_report["autoware"]["recording_style"], "backend_smoke_export")
            self.assertGreater(workflow_report["autoware"]["topic_export_count"], 0)
            self.assertGreater(
                workflow_report["autoware"]["materialized_topic_export_count"], 0
            )
            self.assertIsNotNone(workflow_report["autoware"]["required_topic_count"])
            self.assertIsNotNone(
                workflow_report["autoware"]["missing_required_topic_count"]
            )
            self.assertIsNotNone(workflow_report["autoware"]["subscription_spec_count"])
            self.assertIsNotNone(workflow_report["autoware"]["sensor_input_count"])
            self.assertIsNotNone(workflow_report["autoware"]["static_transform_count"])
            self.assertIsNotNone(workflow_report["autoware"]["processing_stage_count"])
            self.assertIsNotNone(
                workflow_report["autoware"]["ready_processing_stage_count"]
            )
            self.assertIsNotNone(
                workflow_report["autoware"]["degraded_processing_stage_count"]
            )
            self.assertTrue(workflow_report["autoware"]["available_message_types"])
            self.assertIn("camera", workflow_report["autoware"]["available_modalities"])
            self.assertEqual(
                workflow_report["autoware"]["scenario_source"]["variant_id"],
                workflow_report["selection"]["variant_id"],
            )
            self.assertIsNotNone(workflow_report["autoware"]["missing_required_sensor_count"])
            self.assertIn("/sensing/camera/camera_front/image_raw", workflow_report["autoware"]["available_topics"])
            self.assertTrue(Path(workflow_report["artifacts"]["autoware_pipeline_manifest_path"]).is_file())
            self.assertTrue(
                Path(workflow_report["artifacts"]["autoware_topic_export_index_path"]).is_file()
            )
            self.assertTrue(
                Path(workflow_report["artifacts"]["autoware_topic_catalog_path"]).is_file()
            )
            self.assertTrue(
                Path(
                    workflow_report["artifacts"][
                        "autoware_consumer_input_manifest_path"
                    ]
                ).is_file()
            )
            self.assertTrue(workflow_report["autoware"]["topic_export_root"])
            smoke_scenario = json.loads(
                Path(workflow_report["artifacts"]["smoke_scenario_path"]).read_text(encoding="utf-8")
            )
            self.assertEqual(smoke_scenario["objects"][0]["id"], "ego")
            self.assertTrue(Path(workflow_report["smoke"]["summary_path"]).is_file())

    def test_run_scenario_backend_smoke_workflow_exposes_output_mismatch_details(self) -> None:
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
            fake_backend = root / "fake_backend_unexpected.sh"
            _write_fake_backend_unexpected(fake_backend)
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
                selection_strategy="first_successful_variant",
                selected_variant_id="",
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
            self.assertEqual(workflow_report["status"], "SMOKE_FAILED")
            summary = workflow_report["smoke"]["summary"]
            self.assertEqual(summary["output_smoke_status"], "COMPLETE")
            self.assertEqual(summary["output_comparison_status"], "MATCHED")
            self.assertEqual(summary["output_comparison_mismatch_reasons"], [])
            self.assertEqual(summary["output_comparison_unexpected_output_count"], 0)
            self.assertEqual(summary["run_status"], "EXECUTION_FAILED")
            self.assertEqual(workflow_report["autoware"]["status"], "READY")
            self.assertEqual(workflow_report["autoware"]["missing_required_sensor_count"], 0)
            self.assertTrue(Path(workflow_report["artifacts"]["autoware_report_path"]).is_file())

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
                selection_strategy="first_successful_variant",
                selected_variant_id="",
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

    def test_run_scenario_backend_smoke_workflow_auto_discovers_setup_summary(self) -> None:
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
                            "AWSIM_RENDERER_MAP": "Town22",
                        }
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow._discover_runtime_selection_paths",
                return_value={
                    "setup_summary_path": str(setup_summary.resolve()),
                    "backend_workflow_summary_path": None,
                },
            ):
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
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
                workflow_report["runtime_selection"]["setup_summary_path_source"],
                "auto",
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["renderer_map"],
                "Town22",
            )

    def test_run_scenario_backend_smoke_workflow_auto_discovers_package_acquire_summary(
        self,
    ) -> None:
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
            acquire_summary = root / "renderer_backend_package_acquire.json"
            acquire_summary.write_text(
                json.dumps(
                    {
                        "stage": {
                            "selection": {
                                "AWSIM_BIN": str(fake_backend.resolve()),
                                "AWSIM_RENDERER_MAP": "Town31",
                            }
                        }
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow._discover_runtime_selection_paths",
                return_value={
                    "setup_summary_path": None,
                    "backend_workflow_summary_path": None,
                    "package_stage_summary_path": None,
                    "package_acquire_summary_path": str(acquire_summary.resolve()),
                },
            ):
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
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
                "package_acquire_summary",
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["package_acquire_summary_path_source"],
                "auto",
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["renderer_map"],
                "Town31",
            )

    def test_run_scenario_backend_smoke_workflow_can_record_history_guard_pass(self) -> None:
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
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            guard_repo = root / "guard_repo"
            guard_repo.mkdir()
            _init_guard_repo(guard_repo)
            _write_guard_metadata(guard_repo / "metadata" / "autonomy_e2e")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
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
                selection_strategy="first_successful_variant",
                selected_variant_id="",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path="",
                backend_workflow_summary_path="",
                backend_bin="",
                renderer_map="",
                option_overrides=[],
                skip_smoke=True,
                run_history_guard=True,
                history_guard_metadata_root=guard_repo / "metadata" / "autonomy_e2e",
                history_guard_current_repo_root=guard_repo,
                history_guard_compare_ref="origin/main",
                history_guard_include_untracked=False,
            )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "BRIDGED_ONLY")
            self.assertEqual(workflow_report["history_guard"]["status"], "PASS")
            self.assertTrue(Path(workflow_report["artifacts"]["history_guard_report_path"]).is_file())

    def test_run_scenario_backend_smoke_workflow_fails_when_history_guard_fails(self) -> None:
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
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            guard_repo = root / "guard_repo"
            guard_repo.mkdir()
            _init_guard_repo(guard_repo)
            _write_guard_metadata(guard_repo / "metadata" / "autonomy_e2e")
            (guard_repo / "src" / "tracked_module.py").write_text("value = 2\n", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
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
                selection_strategy="first_successful_variant",
                selected_variant_id="",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path="",
                backend_workflow_summary_path="",
                backend_bin="",
                renderer_map="",
                option_overrides=[],
                skip_smoke=True,
                run_history_guard=True,
                history_guard_metadata_root=guard_repo / "metadata" / "autonomy_e2e",
                history_guard_current_repo_root=guard_repo,
                history_guard_compare_ref="origin/main",
                history_guard_include_untracked=False,
            )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "FAILED")
            self.assertEqual(workflow_report["history_guard"]["status"], "FAIL")
            self.assertIn(
                "MIGRATION_CHANGES_WITHOUT_METADATA_REFRESH",
                workflow_report["history_guard"]["failure_codes"],
            )

    def test_run_scenario_backend_smoke_workflow_surfaces_linux_handoff_for_host_incompatible_backend(
        self,
    ) -> None:
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
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "AWSIM-Demo-Lightweight.x86_64"
            fake_backend.write_text("binary\n", encoding="utf-8")
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )
            renderer_workflow_root = root / "renderer_backend_workflow"
            renderer_summary_path = renderer_workflow_root / "renderer_backend_workflow_summary.json"
            renderer_report_path = renderer_workflow_root / "renderer_backend_workflow_report.md"
            linux_handoff_script_path = (
                renderer_workflow_root / "renderer_backend_workflow_linux_handoff.sh"
            )
            linux_handoff_docker_script_path = (
                renderer_workflow_root / "renderer_backend_workflow_linux_handoff_docker.sh"
            )
            bundle_manifest_path = (
                renderer_workflow_root
                / "renderer_backend_workflow_linux_handoff_bundle_manifest.json"
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "EXEC_FORMAT_ERROR",
                    "binary_format": "ELF",
                    "binary_architectures": ["x86_64"],
                    "translation_required": False,
                    "file_description": "ELF 64-bit LSB executable",
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_renderer_backend_workflow",
                return_value={
                    "status": "DRY_RUN_BLOCKED",
                    "success": False,
                    "recommended_next_command": "bash handoff.sh",
                    "blockers": [{"code": "BACKEND_HOST_INCOMPATIBLE"}],
                    "linux_handoff": {
                        "ready": True,
                        "bundle": {"bundle_path": str(root / "handoff_bundle.tar.gz")},
                    },
                    "artifacts": {
                        "summary_path": str(renderer_summary_path),
                        "report_path": str(renderer_report_path),
                        "linux_handoff_script_path": str(linux_handoff_script_path),
                        "linux_handoff_docker_script_path": str(
                            linux_handoff_docker_script_path
                        ),
                        "linux_handoff_bundle_manifest_path": str(bundle_manifest_path),
                    },
                },
            ) as renderer_workflow:
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
                    backend_workflow_summary_path="",
                    backend_bin=str(fake_backend),
                    renderer_map="SampleMap",
                    option_overrides=[],
                    docker_platform="linux/amd64",
                    skip_smoke=False,
                    skip_autoware_bridge=False,
                )

            workflow_report = result["workflow_report"]
            self.assertEqual(renderer_workflow.call_args.kwargs["docker_platform"], "linux/amd64")
            self.assertEqual(workflow_report["status"], "HANDOFF_READY")
            self.assertTrue(workflow_report["renderer_backend_workflow"]["requested"])
            self.assertEqual(
                workflow_report["renderer_backend_workflow"]["status"],
                "DRY_RUN_BLOCKED",
            )
            self.assertTrue(
                workflow_report["renderer_backend_workflow"]["linux_handoff_ready"]
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["backend_host_compatible"],
                False,
            )
            self.assertEqual(
                workflow_report["runtime_selection"]["backend_host_compatibility_reason"],
                "EXEC_FORMAT_ERROR",
            )
            self.assertEqual(
                workflow_report["artifacts"]["renderer_backend_workflow_summary_path"],
                str(renderer_summary_path),
            )
            self.assertEqual(workflow_report["autoware"]["status"], "PLANNED")
            self.assertEqual(workflow_report["autoware"]["availability_mode"], "planned")
            self.assertGreater(workflow_report["autoware"]["topic_export_count"], 0)
            self.assertEqual(
                workflow_report["autoware"]["materialized_topic_export_count"], 0
            )
            self.assertTrue(workflow_report["autoware"]["consumer_ready"])
            self.assertIsNotNone(workflow_report["autoware"]["required_topic_count"])
            self.assertIsNotNone(
                workflow_report["autoware"]["missing_required_topic_count"]
            )
            self.assertTrue(workflow_report["autoware"]["available_message_types"])
            self.assertTrue(
                workflow_report["artifacts"]["autoware_pipeline_manifest_path"]
            )
            self.assertTrue(
                workflow_report["artifacts"]["autoware_topic_export_index_path"]
            )
            self.assertTrue(
                workflow_report["artifacts"]["autoware_topic_catalog_path"]
            )
            self.assertTrue(
                workflow_report["artifacts"]["autoware_consumer_input_manifest_path"]
            )
            self.assertTrue(workflow_report["autoware"]["topic_export_root"])

    def test_run_scenario_backend_smoke_workflow_propagates_handoff_docker_failure(self) -> None:
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
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "AWSIM-Demo-Lightweight.x86_64"
            fake_backend.write_text("binary\n", encoding="utf-8")
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "EXEC_FORMAT_ERROR",
                    "binary_format": "ELF",
                    "binary_architectures": ["x86_64"],
                    "translation_required": False,
                    "file_description": "ELF 64-bit LSB executable",
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_renderer_backend_workflow",
                return_value={
                    "status": "HANDOFF_DOCKER_FAILED",
                    "success": False,
                    "recommended_next_command": "bash handoff.sh",
                    "blockers": [{"code": "BACKEND_HOST_INCOMPATIBLE"}],
                    "linux_handoff": {
                        "ready": True,
                        "bundle": {"bundle_path": str(root / "handoff_bundle.tar.gz")},
                    },
                    "docker_handoff": {"return_code": 1},
                    "artifacts": {},
                },
            ):
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
                    backend_workflow_summary_path="",
                    backend_bin=str(fake_backend),
                    renderer_map="SampleMap",
                    option_overrides=[],
                    run_linux_handoff_docker=True,
                    docker_handoff_execute=True,
                    skip_smoke=False,
                    autoware_consumer_profile="semantic_perception_v0",
                )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "HANDOFF_DOCKER_FAILED")
            self.assertEqual(
                workflow_report["renderer_backend_workflow"]["docker_handoff_status"],
                "HANDOFF_DOCKER_FAILED",
            )

    def test_run_scenario_backend_smoke_workflow_reuses_nested_handoff_smoke_summary(
        self,
    ) -> None:
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
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "AWSIM-Demo-Lightweight.x86_64"
            fake_backend.write_text("binary\n", encoding="utf-8")
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )
            renderer_workflow_root = root / "renderer_backend_workflow"
            renderer_summary_path = (
                renderer_workflow_root / "renderer_backend_workflow_summary.json"
            )
            renderer_summary_path.parent.mkdir(parents=True, exist_ok=True)
            renderer_summary_path.write_text("{}", encoding="utf-8")
            nested_smoke_root = renderer_workflow_root / "linux_handoff" / "smoke_run"
            nested_smoke_root.mkdir(parents=True, exist_ok=True)
            nested_smoke_summary_path = nested_smoke_root / "renderer_backend_smoke_summary.json"
            backend_runner_stdout = (
                nested_smoke_root / "native_only" / "renderer_runtime" / "backend_runner_stdout.log"
            )
            backend_runner_stdout.parent.mkdir(parents=True, exist_ok=True)
            backend_runner_stdout.write_text(
                "\n".join(
                    [
                        "Failed to open plugin: /workspace/runtime/libRobotecGPULidar.so",
                        "Failed to open plugin: /workspace/runtime/libtf2.so",
                        "Forcing GfxDevice: Null",
                        "* Assertion: should not be reached at tramp-amd64.c:641",
                        "Caught fatal signal - signo:6 code:-6 errno:0 addr:0x9",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            nested_smoke_summary_path.write_text(
                json.dumps(
                    {
                        "backend": "awsim",
                        "success": False,
                        "run": {
                            "status": "FAILED",
                            "failure_reason": "OUTPUT_CONTRACT_MISMATCH",
                        },
                        "output_inspection": {"status": "MATCHED"},
                        "output_smoke_report": {
                            "status": "PARTIAL",
                            "coverage_ratio": 0.5,
                        },
                        "output_comparison": {
                            "status": "MISSING_EXPECTED",
                            "mismatch_reasons": ["MISSING_EXPECTED_OUTPUTS"],
                            "unexpected_output_count": 0,
                        },
                        "runner_smoke": {"status": "SMOKE_FAILED", "return_code": -6},
                        "artifacts": {
                            "backend_runner_stdout": str(backend_runner_stdout),
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "EXEC_FORMAT_ERROR",
                    "binary_format": "ELF",
                    "binary_architectures": ["x86_64"],
                    "translation_required": False,
                    "file_description": "ELF 64-bit LSB executable",
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_renderer_backend_workflow",
                return_value={
                    "status": "HANDOFF_DOCKER_FAILED",
                    "success": False,
                    "recommended_next_command": "bash handoff.sh",
                    "blockers": [{"code": "BACKEND_HOST_INCOMPATIBLE"}],
                    "linux_handoff": {
                        "ready": True,
                        "bundle": {"bundle_path": str(root / "handoff_bundle.tar.gz")},
                    },
                    "docker_handoff": {"return_code": 1},
                    "artifacts": {
                        "summary_path": str(renderer_summary_path),
                    },
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_autoware_pipeline_bridge",
                return_value={
                    "report_path": str(root / "autoware" / "autoware_pipeline_bridge_report_v0.json"),
                    "report": {
                        "status": "READY",
                        "available_sensor_count": 3,
                        "missing_required_sensor_count": 0,
                        "available_topics": ["/sensing/camera/camera_front/image_raw"],
                        "required_topics_complete": True,
                        "frame_tree_complete": True,
                        "warnings": [],
                        "artifacts": {
                            "sensor_contracts_path": str(root / "autoware" / "autoware_sensor_contracts.json"),
                            "frame_tree_path": str(root / "autoware" / "autoware_frame_tree.json"),
                            "pipeline_manifest_path": str(root / "autoware" / "autoware_pipeline_manifest.json"),
                            "dataset_manifest_path": str(root / "autoware" / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ) as autoware_bridge:
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
                    backend_workflow_summary_path="",
                    backend_bin=str(fake_backend),
                    renderer_map="SampleMap",
                    option_overrides=[],
                    run_linux_handoff_docker=True,
                    docker_handoff_execute=True,
                    skip_smoke=False,
                    skip_autoware_bridge=False,
                )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "HANDOFF_DOCKER_FAILED")
            self.assertEqual(
                workflow_report["smoke"]["summary_path"],
                str(nested_smoke_summary_path.resolve()),
            )
            self.assertEqual(
                workflow_report["smoke"]["summary"]["output_comparison_status"],
                "MISSING_EXPECTED",
            )
            self.assertEqual(
                workflow_report["smoke"]["summary"]["output_smoke_status"],
                "PARTIAL",
            )
            self.assertEqual(
                workflow_report["smoke"]["summary"]["backend_runtime_exit_code"],
                -6,
            )
            self.assertEqual(
                workflow_report["smoke"]["summary"]["backend_runtime_failed_plugin_count"],
                2,
            )

    def test_run_scenario_backend_smoke_workflow_accepts_handoff_docker_output_ready(self) -> None:
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
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "AWSIM-Demo-Lightweight.x86_64"
            fake_backend.write_text("binary\n", encoding="utf-8")
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "EXEC_FORMAT_ERROR",
                    "binary_format": "ELF",
                    "binary_architectures": ["x86_64"],
                    "translation_required": False,
                    "file_description": "ELF 64-bit LSB executable",
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_renderer_backend_workflow",
                return_value={
                    "status": "HANDOFF_DOCKER_OUTPUT_READY",
                    "success": True,
                    "warning_codes": ["BACKEND_RUNTIME_NONZERO_EXIT_WITH_COMPLETE_OUTPUTS"],
                    "recommended_next_command": "bash handoff.sh",
                    "blockers": [{"code": "BACKEND_HOST_INCOMPATIBLE"}],
                    "linux_handoff": {
                        "ready": True,
                        "bundle": {"bundle_path": str(root / "handoff_bundle.tar.gz")},
                    },
                    "docker_handoff": {"return_code": 1},
                    "artifacts": {},
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_autoware_pipeline_bridge",
                return_value={
                    "report_path": str(root / "autoware" / "autoware_pipeline_bridge_report_v0.json"),
                    "report": {
                        "status": "PLANNED",
                        "availability_mode": "planned",
                        "consumer_profile_id": "semantic_perception_v0",
                        "consumer_profile_description": "semantic perception",
                        "available_sensor_count": 3,
                        "missing_required_sensor_count": 0,
                        "available_topics": ["/sensing/camera/camera_front/image_raw"],
                        "required_topics_complete": True,
                        "frame_tree_complete": True,
                        "warnings": [],
                        "artifacts": {
                            "sensor_contracts_path": str(root / "autoware" / "autoware_sensor_contracts.json"),
                            "frame_tree_path": str(root / "autoware" / "autoware_frame_tree.json"),
                            "pipeline_manifest_path": str(root / "autoware" / "autoware_pipeline_manifest.json"),
                            "dataset_manifest_path": str(root / "autoware" / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ) as autoware_bridge:
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(variant_result["workflow_report_path"]),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
                    backend_workflow_summary_path="",
                    backend_bin=str(fake_backend),
                    renderer_map="SampleMap",
                    option_overrides=[],
                    run_linux_handoff_docker=True,
                    docker_handoff_execute=True,
                    skip_smoke=False,
                    autoware_consumer_profile="semantic_perception_v0",
                )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "HANDOFF_DOCKER_OUTPUT_READY")
            self.assertEqual(
                workflow_report["renderer_backend_workflow"]["docker_handoff_status"],
                "HANDOFF_DOCKER_OUTPUT_READY",
            )
            autoware_bridge.assert_called_once()
            self.assertEqual(
                autoware_bridge.call_args.kwargs["consumer_profile_id"],
                "semantic_perception_v0",
            )
            self.assertEqual(workflow_report["autoware"]["status"], "PLANNED")
            self.assertEqual(workflow_report["autoware"]["availability_mode"], "planned")
            self.assertEqual(
                workflow_report["autoware"]["consumer_profile_id"],
                "semantic_perception_v0",
            )

    def test_run_scenario_backend_smoke_workflow_runs_semantic_supplemental_bridge(
        self,
    ) -> None:
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
                execution_max_variants=1,
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
            first_topic_catalog = root / "autoware_first" / "autoware_topic_catalog.json"
            first_topic_catalog.parent.mkdir(parents=True, exist_ok=True)
            first_topic_catalog.write_text(
                json.dumps(
                    {
                        "missing_required_topics": [
                            "/sensing/camera/camera_front/semantic/image_raw"
                        ]
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            first_report = {
                "status": "DEGRADED",
                "availability_mode": "runtime",
                "consumer_profile_id": "semantic_perception_v0",
                "consumer_profile_description": "semantic perception",
                "available_sensor_count": 2,
                "missing_required_sensor_count": 1,
                "available_topics": ["/sensing/camera/camera_front/image_raw"],
                "required_topics_complete": False,
                "frame_tree_complete": True,
                "warnings": [],
                "artifacts": {
                    "sensor_contracts_path": str(root / "autoware_first" / "autoware_sensor_contracts.json"),
                    "frame_tree_path": str(root / "autoware_first" / "autoware_frame_tree.json"),
                    "pipeline_manifest_path": str(root / "autoware_first" / "autoware_pipeline_manifest.json"),
                    "dataset_manifest_path": str(root / "autoware_first" / "autoware_dataset_manifest.json"),
                    "topic_catalog_path": str(first_topic_catalog),
                },
            }
            second_report = {
                "status": "READY",
                "availability_mode": "runtime",
                "consumer_profile_id": "semantic_perception_v0",
                "consumer_profile_description": "semantic perception",
                "available_sensor_count": 2,
                "missing_required_sensor_count": 0,
                "available_topics": [
                    "/sensing/camera/camera_front/image_raw",
                    "/sensing/camera/camera_front/semantic/image_raw",
                ],
                "required_topics_complete": True,
                "frame_tree_complete": True,
                "warnings": [],
                "merged_report_count": 2,
                "supplemental_backend_smoke_workflow_report_paths": [
                    str(
                        (
                            root
                            / "backend_smoke_workflow"
                            / "supplemental_semantic"
                            / "scenario_backend_smoke_workflow_report_v0.json"
                        ).resolve()
                    )
                ],
                "artifacts": {
                    "sensor_contracts_path": str(root / "autoware" / "autoware_sensor_contracts.json"),
                    "frame_tree_path": str(root / "autoware" / "autoware_frame_tree.json"),
                    "pipeline_manifest_path": str(root / "autoware" / "autoware_pipeline_manifest.json"),
                    "dataset_manifest_path": str(root / "autoware" / "autoware_dataset_manifest.json"),
                    "topic_catalog_path": str(root / "autoware" / "autoware_topic_catalog.json"),
                },
            }

            with patch(
                "hybrid_sensor_sim.tools.scenario_backend_smoke_workflow.run_autoware_pipeline_bridge",
                side_effect=[
                    {
                        "report_path": str(
                            root
                            / "autoware_first"
                            / "autoware_pipeline_bridge_report_v0.json"
                        ),
                        "report": first_report,
                    },
                    {
                        "report_path": str(
                            root / "autoware" / "autoware_pipeline_bridge_report_v0.json"
                        ),
                        "report": second_report,
                    },
                ],
            ) as autoware_bridge:
                result = run_scenario_backend_smoke_workflow(
                    variant_workflow_report_path=str(
                        variant_result["workflow_report_path"]
                    ),
                    batch_workflow_report_path="",
                    smoke_config_path=smoke_config,
                    backend="awsim",
                    out_root=root / "backend_smoke_workflow",
                    selection_strategy="first_successful_variant",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
                    backend_workflow_summary_path="",
                    backend_bin=str(fake_backend),
                    renderer_map="Town07",
                    option_overrides=[],
                    skip_smoke=False,
                    autoware_consumer_profile="semantic_perception_v0",
                )

            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(autoware_bridge.call_count, 2)
            self.assertEqual(workflow_report["autoware"]["status"], "READY")
            self.assertEqual(workflow_report["autoware"]["merged_report_count"], 2)
            self.assertTrue(workflow_report["autoware"]["supplemental_semantic_requested"])
            self.assertEqual(
                workflow_report["autoware"]["supplemental_semantic_status"],
                "SMOKE_SUCCEEDED",
            )
            supplemental_report_path = Path(
                workflow_report["artifacts"][
                    "supplemental_semantic_backend_smoke_workflow_report_path"
                ]
            )
            self.assertTrue(supplemental_report_path.is_file())
            second_call = autoware_bridge.call_args_list[1]
            self.assertTrue(
                second_call.kwargs["supplemental_backend_smoke_workflow_report_paths"]
            )
            supplemental_config = json.loads(
                Path(
                    workflow_report["artifacts"][
                        "supplemental_semantic_smoke_config_path"
                    ]
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                supplemental_config["options"]["camera_sensor_type"],
                "SEMANTIC_SEGMENTATION",
            )
            self.assertFalse(supplemental_config["options"]["lidar_postprocess_enabled"])
            self.assertFalse(supplemental_config["options"]["radar_postprocess_enabled"])

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
