from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
)

from hybrid_sensor_sim.tools.scenario_runtime_backend_workflow import (
    SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
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
            self.assertTrue(Path(report["artifacts"]["smoke_scenario_path"]).is_file())
            self.assertTrue(Path(result["workflow_markdown_path"]).is_file())

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
