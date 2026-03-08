from __future__ import annotations

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

from hybrid_sensor_sim.tools.scenario_runtime_backend_workflow import (
    SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
    main as scenario_runtime_backend_workflow_main,
    run_scenario_runtime_backend_workflow,
)


P_VALIDATION_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"
P_SIM_ENGINE_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"


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
                    "survey_generated_name": "scenario_runtime_backend_test",
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


class ScenarioRuntimeBackendWorkflowTests(unittest.TestCase):
    def _write_attention_logical_scenarios(self, path: Path) -> None:
        collision_scenario = json.loads(
            (P_SIM_ENGINE_FIXTURE_ROOT / "highway_following_v0.json").read_text(encoding="utf-8")
        )
        path.write_text(
            json.dumps(
                {
                    "logical_scenarios": [
                        {
                            "scenario_id": "scn_collision_attention",
                            "parameters": {"scenario_variant": [1]},
                            "variant_payload_kind": "scenario_definition_v0",
                            "variant_payload_template": collision_scenario,
                        }
                    ]
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def test_run_scenario_runtime_backend_workflow_runs_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_success.sh"
            _write_fake_backend_success(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "runtime_backend_workflow",
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
                selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(
                report["scenario_runtime_backend_workflow_report_schema_version"],
                SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(report["batch_workflow"]["status"], "SUCCEEDED")
            self.assertEqual(report["backend_smoke_workflow"]["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(report["status_summary"]["final_status_source"], "default_success")
            self.assertIn(
                report["status_summary"]["autoware_pipeline_status"],
                {"READY", "DEGRADED"},
            )
            self.assertEqual(
                report["status_summary"]["autoware_availability_mode"],
                "runtime",
            )
            self.assertEqual(
                report["status_summary"]["backend_output_origin_status"],
                "BACKEND_RUNTIME_ONLY",
            )
            self.assertTrue(report["status_summary"]["autoware_dataset_ready"])
            self.assertEqual(
                report["status_summary"]["autoware_recording_style"],
                "backend_smoke_export",
            )
            self.assertIn(
                "camera",
                report["status_summary"]["autoware_available_modalities"],
            )
            self.assertTrue(report["status_summary"]["autoware_data_roots"])
            self.assertEqual(
                report["status_summary"]["backend_logical_scenario_id"],
                report["backend_smoke_workflow"]["selection"]["logical_scenario_id"],
            )
            self.assertIsNotNone(report["status_summary"]["autoware_missing_required_sensor_count"])
            self.assertTrue(Path(report["artifacts"]["autoware_pipeline_manifest_path"]).is_file())
            self.assertTrue(Path(report["artifacts"]["smoke_scenario_path"]).is_file())
            self.assertTrue(Path(result["workflow_markdown_path"]).is_file())

    def test_run_scenario_runtime_backend_workflow_surfaces_backend_output_mismatch_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_unexpected.sh"
            _write_fake_backend_unexpected(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "runtime_backend_workflow",
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
                selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["backend_smoke_workflow"]["status"], "SMOKE_FAILED")
            self.assertEqual(
                report["status_summary"]["backend_output_smoke_status"],
                "COMPLETE",
            )
            self.assertEqual(
                report["status_summary"]["backend_output_comparison_status"],
                "MATCHED",
            )
            self.assertEqual(
                report["status_summary"]["backend_output_comparison_mismatch_reasons"],
                [],
            )
            self.assertEqual(
                report["status_summary"]["backend_output_comparison_unexpected_output_count"],
                0,
            )
            self.assertEqual(report["status_summary"]["backend_runner_smoke_status"], "EXECUTION_FAILED")
            self.assertEqual(report["status_summary"]["autoware_pipeline_status"], "READY")
            self.assertTrue(Path(report["artifacts"]["autoware_report_path"]).is_file())

    def test_run_scenario_runtime_backend_workflow_keeps_attention_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_scenarios = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_scenarios)
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_success.sh"
            _write_fake_backend_success(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(logical_scenarios),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="carla",
                out_root=root / "runtime_backend_workflow",
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
                selection_strategy="worst_logical_scenario",
                selected_variant_id="",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path="",
                backend_workflow_summary_path="",
                backend_bin=str(fake_backend),
                renderer_map="Town03",
                option_overrides=[],
                skip_smoke=False,
            )

            report = result["workflow_report"]
            self.assertEqual(report["batch_workflow"]["status"], "ATTENTION")
            self.assertEqual(report["backend_smoke_workflow"]["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(report["status"], "ATTENTION")
            self.assertEqual(report["status_summary"]["final_status_source"], "batch_attention")

    def test_run_scenario_runtime_backend_workflow_supports_skip_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "runtime_backend_workflow",
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
                selection_strategy="worst_logical_scenario",
                selected_variant_id="",
                lane_spacing_m=4.0,
                smoke_output_dir="",
                setup_summary_path="",
                backend_workflow_summary_path="",
                backend_bin="",
                renderer_map="",
                option_overrides=[],
                skip_smoke=True,
            )

            report = result["workflow_report"]
            self.assertEqual(report["status"], "BRIDGED_ONLY")
            self.assertEqual(report["backend_smoke_workflow"]["status"], "BRIDGED_ONLY")
            self.assertEqual(report["status_summary"]["final_status_source"], "smoke_skipped")

    def test_run_scenario_runtime_backend_workflow_can_record_history_guard_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "runtime_backend_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
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
                selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "BRIDGED_ONLY")
            self.assertEqual(report["history_guard"]["status"], "PASS")
            self.assertTrue(Path(report["artifacts"]["history_guard_report_path"]).is_file())

    def test_run_scenario_runtime_backend_workflow_fails_when_history_guard_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="awsim",
                out_root=root / "runtime_backend_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
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
                selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["history_guard"]["status"], "FAIL")
            self.assertEqual(report["status_summary"]["final_status_source"], "history_guard")
            self.assertIn(
                "AUTONOMY_E2E_HISTORY_GUARD_FAILED",
                report["status_summary"]["status_reason_codes"],
            )

    def test_run_scenario_runtime_backend_workflow_uses_setup_summary_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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
                            "CARLA_BIN": str(fake_backend.resolve()),
                            "CARLA_RENDERER_MAP": "Town05",
                        }
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            result = run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                smoke_config_path=smoke_config,
                backend="carla",
                out_root=root / "runtime_backend_workflow",
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
                selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"]["backend_bin_source"],
                "setup_summary",
            )
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"]["renderer_map"],
                "Town05",
            )

    def test_run_scenario_runtime_backend_workflow_auto_discovers_backend_workflow_summary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_success.sh"
            _write_fake_backend_success(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )
            backend_workflow_summary = root / "renderer_backend_workflow_summary.json"
            backend_workflow_summary.write_text(
                json.dumps(
                    {
                        "final_selection": {
                            "CARLA_BIN": str(fake_backend.resolve()),
                            "CARLA_RENDERER_MAP": "Town19",
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
                    "backend_workflow_summary_path": str(
                        backend_workflow_summary.resolve()
                    ),
                },
            ):
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(
                        P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"
                    ),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT
                    / "highway_safe_following_v0.json",
                    smoke_config_path=smoke_config,
                    backend="carla",
                    out_root=root / "runtime_backend_workflow",
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
                    selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"][
                    "backend_workflow_summary_path_source"
                ],
                "auto",
            )
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"][
                    "backend_bin_source"
                ],
                "backend_workflow_summary",
            )
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"]["renderer_map"],
                "Town19",
            )

    def test_run_scenario_runtime_backend_workflow_auto_discovers_package_stage_summary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            fake_backend = root / "fake_backend_success.sh"
            _write_fake_backend_success(fake_backend)
            smoke_config = _write_smoke_base_config(
                root=root,
                helios_bin=fake_helios,
                output_dir=root / "smoke_placeholder",
            )
            package_stage_summary = root / "renderer_backend_package_stage.json"
            package_stage_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "CARLA_BIN": str(fake_backend.resolve()),
                            "CARLA_RENDERER_MAP": "Town27",
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
                    "package_stage_summary_path": str(package_stage_summary.resolve()),
                    "package_acquire_summary_path": None,
                },
            ):
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(
                        P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"
                    ),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT
                    / "highway_safe_following_v0.json",
                    smoke_config_path=smoke_config,
                    backend="carla",
                    out_root=root / "runtime_backend_workflow",
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
                    selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"][
                    "package_stage_summary_path_source"
                ],
                "auto",
            )
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"][
                    "backend_bin_source"
                ],
                "package_stage_summary",
            )
            self.assertEqual(
                report["backend_smoke_workflow"]["runtime_selection"]["renderer_map"],
                "Town27",
            )

    def test_run_scenario_runtime_backend_workflow_promotes_backend_handoff_ready(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch_report_path = root / "batch_workflow_report.json"
            backend_report_path = root / "scenario_backend_smoke_workflow_report_v0.json"
            smoke_scenario_path = root / "smoke_scenario.json"
            smoke_input_config_path = root / "smoke_input_config.json"
            renderer_summary_path = root / "renderer_backend_workflow_summary.json"
            renderer_report_path = root / "renderer_backend_workflow_report.md"
            handoff_script_path = root / "renderer_backend_workflow_linux_handoff.sh"
            bundle_manifest_path = (
                root / "renderer_backend_workflow_linux_handoff_bundle_manifest.json"
            )

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_batch_workflow",
                return_value={
                    "workflow_report_path": batch_report_path,
                    "workflow_markdown_path": root / "batch_workflow_report.md",
                    "workflow_report": {
                        "status": "SUCCEEDED",
                        "status_summary": {
                            "worst_logical_scenario_row": {
                                "logical_scenario_id": "scn_ok"
                            },
                            "gate_failure_codes": [],
                            "status_reason_codes": [],
                        },
                    },
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_backend_smoke_workflow",
                return_value={
                    "workflow_report_path": backend_report_path,
                    "workflow_report": {
                        "status": "HANDOFF_READY",
                        "selection": {"variant_id": "v1"},
                        "runtime_selection": {
                            "backend_bin": "/tmp/AWSIM-Demo-Lightweight.x86_64",
                            "backend_bin_source": "explicit",
                            "backend_host_compatible": False,
                            "backend_host_compatibility_reason": "EXEC_FORMAT_ERROR",
                        },
                        "bridge": {
                            "source_payload_kind": "scenario_definition_v0",
                            "source_payload_path": "/tmp/scenario.json",
                            "smoke_scenario_name": "SCENE",
                            "object_count": 3,
                        },
                        "smoke": {"requested": True},
                        "renderer_backend_workflow": {
                            "status": "DRY_RUN_BLOCKED",
                            "linux_handoff_ready": True,
                            "blocker_codes": ["BACKEND_HOST_INCOMPATIBLE"],
                            "recommended_next_command": "bash handoff.sh",
                            "linux_handoff_bundle_path": "/tmp/handoff_bundle.tar.gz",
                        },
                        "autoware": {
                            "status": "PLANNED",
                            "available_sensor_count": 3,
                            "missing_required_sensor_count": 0,
                            "available_topics": [
                                "/sensing/camera/camera_front/image_raw",
                                "/sensing/lidar/lidar_top/pointcloud",
                            ],
                            "required_topics_complete": True,
                            "frame_tree_complete": True,
                        },
                        "artifacts": {
                            "smoke_scenario_path": str(smoke_scenario_path),
                            "smoke_input_config_path": str(smoke_input_config_path),
                            "renderer_backend_workflow_summary_path": str(
                                renderer_summary_path
                            ),
                            "renderer_backend_workflow_report_path": str(
                                renderer_report_path
                            ),
                            "renderer_backend_linux_handoff_script_path": str(
                                handoff_script_path
                            ),
                            "renderer_backend_linux_handoff_bundle_manifest_path": str(
                                bundle_manifest_path
                            ),
                            "autoware_report_path": str(root / "autoware_report.json"),
                            "autoware_sensor_contracts_path": str(root / "autoware_sensor_contracts.json"),
                            "autoware_frame_tree_path": str(root / "autoware_frame_tree.json"),
                            "autoware_pipeline_manifest_path": str(root / "autoware_pipeline_manifest.json"),
                            "autoware_dataset_manifest_path": str(root / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ):
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(
                        P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"
                    ),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT
                    / "highway_safe_following_v0.json",
                    smoke_config_path=root / "smoke_config.json",
                    backend="awsim",
                    out_root=root / "runtime_backend_workflow",
                    sampling="full",
                    sample_size=0,
                    seed=7,
                    max_variants_per_scenario=1000,
                    execution_max_variants=1,
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
                    selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "HANDOFF_READY")
            self.assertEqual(
                report["status_summary"]["final_status_source"],
                "backend_handoff_ready",
            )
            self.assertEqual(
                report["status_summary"]["backend_handoff_status"],
                "DRY_RUN_BLOCKED",
            )
            self.assertTrue(report["status_summary"]["backend_handoff_ready"])
            self.assertIn(
                "BACKEND_HOST_INCOMPATIBLE",
                report["status_summary"]["backend_handoff_blocker_codes"],
            )
            self.assertEqual(
                report["status_summary"]["autoware_pipeline_status"],
                "PLANNED",
            )
            self.assertEqual(
                report["status_summary"]["autoware_availability_mode"],
                "planned",
            )
            self.assertTrue(
                report["status_summary"]["autoware_required_topics_complete"]
            )
            self.assertEqual(
                report["artifacts"]["renderer_backend_linux_handoff_script_path"],
                str(handoff_script_path),
            )

    def test_run_scenario_runtime_backend_workflow_fails_for_backend_handoff_docker_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_batch_workflow",
                return_value={
                    "workflow_report_path": root / "batch_workflow_report.json",
                    "workflow_markdown_path": root / "batch_workflow_report.md",
                    "workflow_report": {
                        "status": "ATTENTION",
                        "status_summary": {
                            "worst_logical_scenario_row": {"logical_scenario_id": "scn_attention"},
                            "gate_failure_codes": [],
                            "status_reason_codes": [],
                        },
                    },
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_backend_smoke_workflow",
                return_value={
                    "workflow_report_path": root / "scenario_backend_smoke_workflow_report_v0.json",
                    "workflow_report": {
                        "status": "HANDOFF_DOCKER_FAILED",
                        "selection": {"variant_id": "v1"},
                        "smoke": {
                            "summary": {
                                "backend_runtime_exit_code": -6,
                                "backend_runtime_failed_plugin_count": 2,
                                "backend_runtime_failed_plugins": [
                                    "libRobotecGPULidar.so",
                                    "libtf2.so",
                                ],
                                "backend_runtime_missing_shared_libraries": [],
                                "backend_runtime_crash_signatures": [
                                    "NULL_GFX_DEVICE",
                                    "MONO_TRAMP_AMD64_ASSERT",
                                    "SIGABRT",
                                    "PLUGIN_LOAD_FAILURES",
                                ],
                            }
                        },
                        "renderer_backend_workflow": {
                            "status": "HANDOFF_DOCKER_FAILED",
                            "linux_handoff_ready": True,
                            "blocker_codes": ["BACKEND_HOST_INCOMPATIBLE"],
                            "recommended_next_command": "bash handoff.sh",
                            "linux_handoff_bundle_path": "/tmp/handoff_bundle.tar.gz",
                        },
                        "autoware": {
                            "status": "PLANNED",
                            "available_sensor_count": 3,
                            "missing_required_sensor_count": 0,
                            "available_topics": [],
                            "required_topics_complete": True,
                            "frame_tree_complete": True,
                        },
                        "artifacts": {
                            "smoke_scenario_path": str(root / "smoke_scenario.json"),
                            "smoke_input_config_path": str(root / "smoke_input_config.json"),
                            "autoware_report_path": str(root / "autoware_report.json"),
                            "autoware_sensor_contracts_path": str(root / "autoware_sensor_contracts.json"),
                            "autoware_frame_tree_path": str(root / "autoware_frame_tree.json"),
                            "autoware_pipeline_manifest_path": str(root / "autoware_pipeline_manifest.json"),
                            "autoware_dataset_manifest_path": str(root / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ):
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                    smoke_config_path=root / "smoke_config.json",
                    backend="awsim",
                    out_root=root / "runtime_backend_workflow",
                    sampling="full",
                    sample_size=0,
                    seed=7,
                    max_variants_per_scenario=1000,
                    execution_max_variants=1,
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
                    selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(
                report["status_summary"]["final_status_source"],
                "backend_handoff_docker_failed",
            )
            self.assertIn(
                "BACKEND_HANDOFF_DOCKER_FAILED",
                report["status_summary"]["status_reason_codes"],
            )
            self.assertEqual(report["status_summary"]["backend_runtime_exit_code"], -6)
            self.assertEqual(
                report["status_summary"]["backend_runtime_failed_plugins"],
                ["libRobotecGPULidar.so", "libtf2.so"],
            )
            self.assertEqual(
                report["status_summary"]["backend_runtime_crash_signatures"],
                [
                    "NULL_GFX_DEVICE",
                    "MONO_TRAMP_AMD64_ASSERT",
                    "SIGABRT",
                    "PLUGIN_LOAD_FAILURES",
                ],
            )

    def test_run_scenario_runtime_backend_workflow_promotes_usable_output_ready_handoff(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_batch_workflow",
                return_value={
                    "workflow_report_path": root / "batch_workflow_report.json",
                    "workflow_markdown_path": root / "batch_workflow_report.md",
                    "workflow_report": {
                        "status": "SUCCEEDED",
                        "status_summary": {
                            "worst_logical_scenario_row": {"logical_scenario_id": "scn_ok"},
                            "gate_failure_codes": [],
                            "status_reason_codes": [],
                        },
                    },
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_backend_smoke_workflow",
                return_value={
                    "workflow_report_path": root / "scenario_backend_smoke_workflow_report_v0.json",
                    "workflow_report": {
                        "status": "HANDOFF_DOCKER_OUTPUT_READY",
                        "selection": {"variant_id": "v1"},
                        "smoke": {
                            "summary": {
                                "backend_runtime_exit_code": 1,
                                "output_comparison_status": "MATCHED",
                                "output_smoke_status": "COMPLETE",
                                "output_origin_status": "BACKEND_RUNTIME_ONLY",
                            }
                        },
                        "renderer_backend_workflow": {
                            "status": "HANDOFF_DOCKER_OUTPUT_READY",
                            "linux_handoff_ready": True,
                            "blocker_codes": ["BACKEND_HOST_INCOMPATIBLE"],
                            "recommended_next_command": "bash handoff.sh",
                            "linux_handoff_bundle_path": "/tmp/handoff_bundle.tar.gz",
                            "warning_codes": [
                                "BACKEND_RUNTIME_NONZERO_EXIT_WITH_COMPLETE_OUTPUTS"
                            ],
                        },
                        "autoware": {
                            "status": "READY",
                            "availability_mode": "runtime",
                            "available_sensor_count": 3,
                            "missing_required_sensor_count": 0,
                            "available_topics": ["/sensing/lidar/lidar_top/pointcloud"],
                            "required_topics_complete": True,
                            "frame_tree_complete": True,
                        },
                        "artifacts": {
                            "smoke_scenario_path": str(root / "smoke_scenario.json"),
                            "smoke_input_config_path": str(root / "smoke_input_config.json"),
                            "autoware_report_path": str(root / "autoware_report.json"),
                            "autoware_sensor_contracts_path": str(root / "autoware_sensor_contracts.json"),
                            "autoware_frame_tree_path": str(root / "autoware_frame_tree.json"),
                            "autoware_pipeline_manifest_path": str(root / "autoware_pipeline_manifest.json"),
                            "autoware_dataset_manifest_path": str(root / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ):
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                    smoke_config_path=root / "smoke_config.json",
                    backend="awsim",
                    out_root=root / "runtime_backend_workflow",
                    sampling="full",
                    sample_size=0,
                    seed=7,
                    max_variants_per_scenario=1000,
                    execution_max_variants=1,
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
                    selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(
                report["status_summary"]["final_status_source"],
                "backend_handoff_docker_output_usable",
            )
            self.assertIn(
                "BACKEND_HANDOFF_DOCKER_OUTPUT_USABLE",
                report["status_summary"]["status_reason_codes"],
            )
            self.assertEqual(
                report["status_summary"]["autoware_pipeline_status"],
                "READY",
            )
            self.assertTrue(report["status_summary"]["backend_output_usable"])
            self.assertEqual(
                report["status_summary"]["backend_handoff_warning_codes"],
                ["BACKEND_RUNTIME_NONZERO_EXIT_WITH_COMPLETE_OUTPUTS"],
            )

    def test_run_scenario_runtime_backend_workflow_keeps_non_runtime_output_ready_handoff_as_attention(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_batch_workflow",
                return_value={
                    "workflow_report_path": root / "batch_workflow_report.json",
                    "workflow_markdown_path": root / "batch_workflow_report.md",
                    "workflow_report": {
                        "status": "SUCCEEDED",
                        "status_summary": {
                            "worst_logical_scenario_row": {"logical_scenario_id": "scn_ok"},
                            "gate_failure_codes": [],
                            "status_reason_codes": [],
                        },
                    },
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_backend_smoke_workflow",
                return_value={
                    "workflow_report_path": root / "scenario_backend_smoke_workflow_report_v0.json",
                    "workflow_report": {
                        "status": "HANDOFF_DOCKER_OUTPUT_READY",
                        "selection": {"variant_id": "v1"},
                        "smoke": {
                            "summary": {
                                "backend_runtime_exit_code": 1,
                                "output_comparison_status": "MATCHED",
                                "output_smoke_status": "COMPLETE",
                                "output_origin_status": "BACKEND_RUNTIME_ONLY",
                            }
                        },
                        "renderer_backend_workflow": {
                            "status": "HANDOFF_DOCKER_OUTPUT_READY",
                            "linux_handoff_ready": True,
                            "blocker_codes": ["BACKEND_HOST_INCOMPATIBLE"],
                            "warning_codes": [
                                "BACKEND_RUNTIME_NONZERO_EXIT_WITH_COMPLETE_OUTPUTS"
                            ],
                        },
                        "autoware": {
                            "status": "SIDECAR_READY",
                            "availability_mode": "sidecar",
                            "available_sensor_count": 3,
                            "missing_required_sensor_count": 0,
                            "available_topics": ["/sensing/lidar/lidar_top/pointcloud"],
                            "required_topics_complete": True,
                            "frame_tree_complete": True,
                        },
                        "artifacts": {
                            "smoke_scenario_path": str(root / "smoke_scenario.json"),
                            "smoke_input_config_path": str(root / "smoke_input_config.json"),
                            "autoware_report_path": str(root / "autoware_report.json"),
                            "autoware_sensor_contracts_path": str(root / "autoware_sensor_contracts.json"),
                            "autoware_frame_tree_path": str(root / "autoware_frame_tree.json"),
                            "autoware_pipeline_manifest_path": str(root / "autoware_pipeline_manifest.json"),
                            "autoware_dataset_manifest_path": str(root / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ):
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                    smoke_config_path=root / "smoke_config.json",
                    backend="awsim",
                    out_root=root / "runtime_backend_workflow",
                    sampling="full",
                    sample_size=0,
                    seed=7,
                    max_variants_per_scenario=1000,
                    execution_max_variants=1,
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
                    selection_strategy="worst_logical_scenario",
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

            report = result["workflow_report"]
            self.assertEqual(report["status"], "ATTENTION")
            self.assertEqual(
                report["status_summary"]["final_status_source"],
                "backend_handoff_docker_output_ready",
            )
            self.assertIn(
                "BACKEND_HANDOFF_DOCKER_OUTPUT_READY",
                report["status_summary"]["status_reason_codes"],
            )
            self.assertFalse(report["status_summary"]["backend_output_usable"])

    def test_run_scenario_runtime_backend_workflow_forwards_docker_platform(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_batch_workflow",
                return_value={
                    "workflow_report_path": root / "batch_workflow_report.json",
                    "workflow_markdown_path": root / "batch_workflow_report.md",
                    "workflow_report": {
                        "status": "SUCCEEDED",
                        "status_summary": {
                            "worst_logical_scenario_row": {"logical_scenario_id": "scn_ok"},
                            "gate_failure_codes": [],
                            "status_reason_codes": [],
                        },
                    },
                },
            ), patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_backend_smoke_workflow",
                return_value={
                    "workflow_report_path": root / "scenario_backend_smoke_workflow_report_v0.json",
                    "workflow_report": {
                        "status": "BRIDGED_ONLY",
                        "selection": {"variant_id": "v1"},
                        "smoke": {"summary": {}},
                        "renderer_backend_workflow": {},
                        "autoware": {
                            "status": "PLANNED",
                            "available_sensor_count": 0,
                            "missing_required_sensor_count": 0,
                            "available_topics": [],
                            "required_topics_complete": False,
                            "frame_tree_complete": True,
                        },
                        "artifacts": {
                            "smoke_scenario_path": str(root / "smoke_scenario.json"),
                            "smoke_input_config_path": str(root / "smoke_input_config.json"),
                            "autoware_report_path": str(root / "autoware_report.json"),
                            "autoware_sensor_contracts_path": str(root / "autoware_sensor_contracts.json"),
                            "autoware_frame_tree_path": str(root / "autoware_frame_tree.json"),
                            "autoware_pipeline_manifest_path": str(root / "autoware_pipeline_manifest.json"),
                            "autoware_dataset_manifest_path": str(root / "autoware_dataset_manifest.json"),
                        },
                    },
                },
            ) as smoke_workflow:
                result = run_scenario_runtime_backend_workflow(
                    logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                    scenario_language_profile="",
                    scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                    matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                    smoke_config_path=root / "smoke_config.json",
                    backend="awsim",
                    out_root=root / "runtime_backend_workflow",
                    sampling="full",
                    sample_size=0,
                    seed=7,
                    max_variants_per_scenario=1000,
                    execution_max_variants=1,
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
                    selection_strategy="worst_logical_scenario",
                    selected_variant_id="",
                    lane_spacing_m=4.0,
                    smoke_output_dir="",
                    setup_summary_path="",
                    backend_workflow_summary_path="",
                    backend_bin="",
                    renderer_map="",
                    option_overrides=[],
                    run_linux_handoff_docker=True,
                    docker_platform="linux/amd64",
                    skip_smoke=True,
                )

            self.assertEqual(smoke_workflow.call_args.kwargs["docker_platform"], "linux/amd64")
            self.assertEqual(result["workflow_report"]["status"], "BRIDGED_ONLY")

    def test_scenario_runtime_backend_workflow_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_scenario_runtime_backend_workflow.py"
        )
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("Run scenario batch workflow", completed.stdout)

    def test_scenario_runtime_backend_workflow_main_accepts_gate_profile_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            smoke_config = root / "smoke_config.json"
            smoke_config.write_text("{}", encoding="utf-8")
            workflow_root = root / "workflow"

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_workflow.run_scenario_runtime_backend_workflow",
                return_value={
                    "workflow_report": {
                        "status": "SUCCEEDED",
                        "batch_workflow": {"status": "SUCCEEDED"},
                        "backend_smoke_workflow": {"status": "BRIDGED_ONLY"},
                    },
                    "workflow_report_path": workflow_root / "scenario_runtime_backend_workflow_report_v0.json",
                },
            ) as mocked_run:
                exit_code = scenario_runtime_backend_workflow_main(
                    [
                        "--scenario-language-profile",
                        "highway_mixed_payloads_v0",
                        "--matrix-scenario",
                        str(P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json"),
                        "--smoke-config",
                        str(smoke_config),
                        "--backend",
                        "awsim",
                        "--out-root",
                        str(workflow_root),
                        "--gate-profile-id",
                        "scenario_batch_gate_strict_v0",
                        "--skip-smoke",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertTrue(mocked_run.called)
