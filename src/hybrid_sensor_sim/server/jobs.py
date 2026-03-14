from __future__ import annotations

import json
import mimetypes
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from hybrid_sensor_sim.server.db import ControlPlaneDB, utc_now_iso
from hybrid_sensor_sim.tools import (
    build_autonomy_e2e_history_guard_report,
    build_scenario_batch_comparison_report,
    build_renderer_backend_local_setup,
    run_autoware_pipeline_bridge,
    run_log_replay,
    run_object_sim_job,
    run_scenario_backend_smoke_workflow,
    run_scenario_runtime_backend_probe_set,
    run_scenario_runtime_backend_rebridge,
    run_scenario_runtime_backend_workflow,
    run_scenario_variant_workflow,
)
from hybrid_sensor_sim.tools.scenario_batch_workflow import run_scenario_batch_workflow

RunCallable = Callable[[dict[str, Any]], dict[str, Any]]

DEFAULT_ARTIFACT_ROOT = Path(__file__).resolve().parents[3] / "artifacts" / "control_plane" / "runs"


class JobManager:
    def __init__(self, db: ControlPlaneDB, repo_root: Path) -> None:
        self.db = db
        self.repo_root = repo_root.resolve()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="control-plane")
        self._lock = threading.Lock()

    def submit(self, *, run_type: str, project_id: str, payload: dict[str, Any], source_kind: str = "api_request") -> dict[str, Any]:
        run_id = str(payload.get("run_id") or self._build_run_id(run_type))
        artifact_root = Path(payload.get("out_root") or (DEFAULT_ARTIFACT_ROOT / run_id)).resolve()
        artifact_root.mkdir(parents=True, exist_ok=True)
        normalized_payload = self._json_safe(payload)
        run_row = self.db.create_run(
            run_id=run_id,
            run_type=run_type,
            project_id=project_id,
            source_kind=source_kind,
            artifact_root=str(artifact_root),
            request_payload=normalized_payload,
        )
        self.executor.submit(self._execute_run, run_id, run_type, payload, artifact_root)
        return run_row

    def _build_run_id(self, run_type: str) -> str:
        token = uuid4().hex[:8].upper()
        return f"CP_{run_type.upper()}_{token}"

    def _execute_run(self, run_id: str, run_type: str, payload: dict[str, Any], artifact_root: Path) -> None:
        self.db.update_run(run_id, status="RUNNING", started_at=utc_now_iso())
        try:
            result = self._dispatch(run_type, payload, artifact_root)
            normalized_result = self._json_safe(result)
            summary_json_path, summary_markdown_path, recommended_next_command, status, reason_codes = self._summarize_result(run_type, result)
            artifacts = self._index_artifacts(artifact_root, result)
            self.db.replace_run_artifacts(run_id, artifacts)
            self.db.update_run(
                run_id,
                status=status,
                finished_at=utc_now_iso(),
                status_reason_codes=reason_codes,
                summary_json_path=summary_json_path,
                summary_markdown_path=summary_markdown_path,
                recommended_next_command=recommended_next_command,
                result_payload=normalized_result,
                error_message="",
            )
        except Exception as exc:
            artifacts = self._index_artifacts(artifact_root, {})
            self.db.replace_run_artifacts(run_id, artifacts)
            self.db.update_run(
                run_id,
                status="FAILED",
                finished_at=utc_now_iso(),
                status_reason_codes=["RUN_EXCEPTION"],
                error_message=str(exc),
                result_payload=self._json_safe({"error": str(exc)}),
            )

    def _resolve_repo_path(self, path_text: str) -> Path:
        candidate = Path(str(path_text).strip()).expanduser()
        if candidate.is_absolute():
            return candidate.resolve()
        return (self.repo_root / candidate).resolve()

    def _payload_value(self, payload: dict[str, Any], *keys: str, default: Any = "") -> Any:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
        return default

    def _dispatch(self, run_type: str, payload: dict[str, Any], artifact_root: Path) -> dict[str, Any]:
        if run_type == "object_sim":
            return run_object_sim_job(
                scenario_path=self._resolve_repo_path(payload["scenario_path"]),
                run_id=str(payload.get("run_id") or artifact_root.name),
                out_root=artifact_root,
                seed=int(payload.get("seed", 7)),
                wall_timeout_override=payload.get("wall_timeout_sec"),
                metadata=payload.get("metadata") or {},
            )
        if run_type == "batch_workflow":
            return run_scenario_batch_workflow(
                logical_scenarios_path=str(payload.get("logical_scenarios_path", "")),
                scenario_language_profile=str(payload.get("scenario_language_profile", "")),
                scenario_language_dir=payload.get("scenario_language_dir"),
                matrix_scenario_path=self._resolve_repo_path(self._payload_value(payload, "matrix_scenario_path", "matrix_scenario")),
                out_root=artifact_root,
                sampling=str(payload.get("sampling", "full")),
                sample_size=int(payload.get("sample_size", 1)),
                seed=int(payload.get("seed", 7)),
                max_variants_per_scenario=int(payload.get("max_variants_per_scenario", 4)),
                execution_max_variants=int(payload.get("execution_max_variants", 1)),
                sds_version=str(payload.get("sds_version", "1.0.0")),
                sim_version=str(payload.get("sim_version", "0.1.0")),
                fidelity_profile=str(payload.get("fidelity_profile", "native_v0")),
                matrix_run_id_prefix=str(payload.get("matrix_run_id_prefix", "RUN_CORE_SIM_SWEEP")),
                traffic_profile_ids=list(payload.get("traffic_profile_ids", ["sumo_highway_balanced_v0"])),
                traffic_actor_pattern_ids=list(payload.get("traffic_actor_pattern_ids", ["sumo_platoon_sparse_v0"])),
                traffic_npc_speed_scale_values=list(payload.get("traffic_npc_speed_scale_values", [1.0])),
                tire_friction_coeff_values=list(payload.get("tire_friction_coeff_values", [1.0])),
                surface_friction_scale_values=list(payload.get("surface_friction_scale_values", [1.0])),
                enable_ego_collision_avoidance=bool(payload.get("enable_ego_collision_avoidance", True)),
                avoidance_ttc_threshold_sec=float(payload.get("avoidance_ttc_threshold_sec", 3.0)),
                ego_max_brake_mps2=float(payload.get("ego_max_brake_mps2", 6.0)),
                max_cases=int(payload.get("max_cases", 1)),
                gate_profile_path=self._resolve_repo_path(payload["gate_profile_path"]) if payload.get("gate_profile_path") else None,
                gate_max_attention_rows=payload.get("gate_max_attention_rows"),
                gate_max_collision_rows=payload.get("gate_max_collision_rows"),
                gate_max_timeout_rows=payload.get("gate_max_timeout_rows"),
                gate_min_min_ttc_any_lane_sec=payload.get("gate_min_min_ttc_any_lane_sec"),
                gate_max_path_conflict_rows=payload.get("gate_max_path_conflict_rows"),
                gate_max_merge_conflict_rows=payload.get("gate_max_merge_conflict_rows"),
                gate_max_lane_change_conflict_rows=payload.get("gate_max_lane_change_conflict_rows"),
                gate_min_min_ttc_path_conflict_sec=payload.get("gate_min_min_ttc_path_conflict_sec"),
                gate_max_avoidance_rows=payload.get("gate_max_avoidance_rows"),
                gate_max_avoidance_brake_events=payload.get("gate_max_avoidance_brake_events"),
                gate_max_avoidance_same_lane_conflict_triggers=payload.get("gate_max_avoidance_same_lane_conflict_triggers"),
                gate_max_avoidance_merge_conflict_triggers=payload.get("gate_max_avoidance_merge_conflict_triggers"),
                gate_max_avoidance_lane_change_conflict_triggers=payload.get("gate_max_avoidance_lane_change_conflict_triggers"),
                gate_max_avoidance_downstream_route_conflict_triggers=payload.get("gate_max_avoidance_downstream_route_conflict_triggers"),
                fail_on_attention=bool(payload.get("fail_on_attention", False)),
            )
        if run_type == "backend_smoke":
            return run_scenario_backend_smoke_workflow(
                variant_workflow_report_path=str(payload.get("variant_workflow_report_path", "")),
                batch_workflow_report_path=str(payload.get("batch_workflow_report_path", "")),
                smoke_config_path=self._resolve_repo_path(self._payload_value(payload, "smoke_config_path", "config")),
                backend=str(payload.get("backend", "awsim")),
                out_root=artifact_root,
                selection_strategy=str(payload.get("selection_strategy", "first_succeeded")),
                selected_variant_id=str(payload.get("selected_variant_id", "")),
                lane_spacing_m=float(payload.get("lane_spacing_m", 3.7)),
                smoke_output_dir=str(payload.get("smoke_output_dir", "")),
                setup_summary_path=str(payload.get("setup_summary_path", "")),
                backend_workflow_summary_path=str(payload.get("backend_workflow_summary_path", "")),
                backend_bin=str(payload.get("backend_bin", "")),
                renderer_map=str(payload.get("renderer_map", "")),
                option_overrides=list(payload.get("option_overrides", [])),
                renderer_backend_workflow_output_root=str(payload.get("renderer_backend_workflow_output_root", "")),
                pack_linux_handoff=bool(payload.get("pack_linux_handoff", False)),
                verify_linux_handoff_bundle=bool(payload.get("verify_linux_handoff_bundle", False)),
                run_linux_handoff_docker=bool(payload.get("run_linux_handoff_docker", False)),
                docker_handoff_execute=bool(payload.get("docker_handoff_execute", False)),
                docker_binary=str(payload.get("docker_binary", "docker")),
                docker_image=str(payload.get("docker_image", "python:3.11-slim")),
                docker_platform=payload.get("docker_platform"),
                docker_container_workspace=str(payload.get("docker_container_workspace", "/workspace")),
                refresh_docker_handoff_preflight=bool(payload.get("refresh_docker_handoff_preflight", False)),
                skip_smoke=bool(payload.get("skip_smoke", False)),
                skip_autoware_bridge=bool(payload.get("skip_autoware_bridge", False)),
                autoware_base_frame=str(payload.get("autoware_base_frame", "base_link")),
                autoware_consumer_profile=str(payload.get("autoware_consumer_profile", "")),
                autoware_semantic_supplemental_strategy=str(payload.get("autoware_semantic_supplemental_strategy", "auto")),
                autoware_strict=bool(payload.get("autoware_strict", False)),
                run_history_guard=bool(payload.get("run_history_guard", False)),
                history_guard_metadata_root=payload.get("history_guard_metadata_root"),
                history_guard_current_repo_root=payload.get("history_guard_current_repo_root"),
                history_guard_compare_ref=str(payload.get("history_guard_compare_ref", "origin/main")),
                history_guard_include_untracked=bool(payload.get("history_guard_include_untracked", False)),
                enable_semantic_supplemental=bool(payload.get("enable_semantic_supplemental", True)),
            )
        if run_type == "runtime_backend":
            return run_scenario_runtime_backend_workflow(
                logical_scenarios_path=str(payload.get("logical_scenarios_path", "")),
                scenario_language_profile=str(payload.get("scenario_language_profile", "")),
                scenario_language_dir=payload.get("scenario_language_dir"),
                matrix_scenario_path=self._resolve_repo_path(self._payload_value(payload, "matrix_scenario_path", "matrix_scenario")),
                smoke_config_path=self._resolve_repo_path(self._payload_value(payload, "smoke_config_path", "smoke_config")),
                backend=str(payload.get("backend", "awsim")),
                out_root=artifact_root,
                sampling=str(payload.get("sampling", "full")),
                sample_size=int(payload.get("sample_size", 1)),
                seed=int(payload.get("seed", 7)),
                max_variants_per_scenario=int(payload.get("max_variants_per_scenario", 4)),
                execution_max_variants=int(payload.get("execution_max_variants", 1)),
                sds_version=str(payload.get("sds_version", "1.0.0")),
                sim_version=str(payload.get("sim_version", "0.1.0")),
                fidelity_profile=str(payload.get("fidelity_profile", "native_v0")),
                matrix_run_id_prefix=str(payload.get("matrix_run_id_prefix", "RUN_CORE_SIM_SWEEP")),
                traffic_profile_ids=list(payload.get("traffic_profile_ids", ["sumo_highway_balanced_v0"])),
                traffic_actor_pattern_ids=list(payload.get("traffic_actor_pattern_ids", ["sumo_platoon_sparse_v0"])),
                traffic_npc_speed_scale_values=list(payload.get("traffic_npc_speed_scale_values", [1.0])),
                tire_friction_coeff_values=list(payload.get("tire_friction_coeff_values", [1.0])),
                surface_friction_scale_values=list(payload.get("surface_friction_scale_values", [1.0])),
                enable_ego_collision_avoidance=bool(payload.get("enable_ego_collision_avoidance", True)),
                avoidance_ttc_threshold_sec=float(payload.get("avoidance_ttc_threshold_sec", 3.0)),
                ego_max_brake_mps2=float(payload.get("ego_max_brake_mps2", 6.0)),
                max_cases=int(payload.get("max_cases", 1)),
                gate_profile_path=self._resolve_repo_path(payload["gate_profile_path"]) if payload.get("gate_profile_path") else None,
                gate_max_attention_rows=payload.get("gate_max_attention_rows"),
                gate_max_collision_rows=payload.get("gate_max_collision_rows"),
                gate_max_timeout_rows=payload.get("gate_max_timeout_rows"),
                gate_min_min_ttc_any_lane_sec=payload.get("gate_min_min_ttc_any_lane_sec"),
                gate_max_path_conflict_rows=payload.get("gate_max_path_conflict_rows"),
                gate_max_merge_conflict_rows=payload.get("gate_max_merge_conflict_rows"),
                gate_max_lane_change_conflict_rows=payload.get("gate_max_lane_change_conflict_rows"),
                gate_min_min_ttc_path_conflict_sec=payload.get("gate_min_min_ttc_path_conflict_sec"),
                gate_max_avoidance_rows=payload.get("gate_max_avoidance_rows"),
                gate_max_avoidance_brake_events=payload.get("gate_max_avoidance_brake_events"),
                gate_max_avoidance_same_lane_conflict_triggers=payload.get("gate_max_avoidance_same_lane_conflict_triggers"),
                gate_max_avoidance_merge_conflict_triggers=payload.get("gate_max_avoidance_merge_conflict_triggers"),
                gate_max_avoidance_lane_change_conflict_triggers=payload.get("gate_max_avoidance_lane_change_conflict_triggers"),
                gate_max_avoidance_downstream_route_conflict_triggers=payload.get("gate_max_avoidance_downstream_route_conflict_triggers"),
                selection_strategy=str(payload.get("selection_strategy", "first_succeeded")),
                selected_variant_id=str(payload.get("selected_variant_id", "")),
                lane_spacing_m=float(payload.get("lane_spacing_m", 3.7)),
                smoke_output_dir=str(payload.get("smoke_output_dir", "")),
                setup_summary_path=str(payload.get("setup_summary_path", "")),
                backend_workflow_summary_path=str(payload.get("backend_workflow_summary_path", "")),
                backend_bin=str(payload.get("backend_bin", "")),
                renderer_map=str(payload.get("renderer_map", "")),
                option_overrides=list(payload.get("option_overrides", [])),
                renderer_backend_workflow_output_root=str(payload.get("renderer_backend_workflow_output_root", "")),
                pack_linux_handoff=bool(payload.get("pack_linux_handoff", False)),
                verify_linux_handoff_bundle=bool(payload.get("verify_linux_handoff_bundle", False)),
                run_linux_handoff_docker=bool(payload.get("run_linux_handoff_docker", False)),
                docker_handoff_execute=bool(payload.get("docker_handoff_execute", False)),
                docker_binary=str(payload.get("docker_binary", "docker")),
                docker_image=str(payload.get("docker_image", "python:3.11-slim")),
                docker_platform=payload.get("docker_platform"),
                docker_container_workspace=str(payload.get("docker_container_workspace", "/workspace")),
                refresh_docker_handoff_preflight=bool(payload.get("refresh_docker_handoff_preflight", False)),
                skip_smoke=bool(payload.get("skip_smoke", False)),
                skip_autoware_bridge=bool(payload.get("skip_autoware_bridge", False)),
                autoware_base_frame=str(payload.get("autoware_base_frame", "base_link")),
                autoware_consumer_profile=str(payload.get("autoware_consumer_profile", "")),
                autoware_semantic_supplemental_strategy=str(payload.get("autoware_semantic_supplemental_strategy", "auto")),
                autoware_strict=bool(payload.get("autoware_strict", False)),
                run_history_guard=bool(payload.get("run_history_guard", False)),
                history_guard_metadata_root=payload.get("history_guard_metadata_root"),
                history_guard_current_repo_root=payload.get("history_guard_current_repo_root"),
                history_guard_compare_ref=str(payload.get("history_guard_compare_ref", "origin/main")),
                history_guard_include_untracked=bool(payload.get("history_guard_include_untracked", False)),
            )
        if run_type == "rebridge":
            supplemental = payload.get("supplemental_backend_smoke_workflow_report_paths") or []
            return run_scenario_runtime_backend_rebridge(
                runtime_backend_workflow_report_path=str(self._payload_value(payload, "runtime_backend_workflow_report_path", "runtime_backend_workflow_report")),
                backend_smoke_workflow_report_path=str(payload.get("backend_smoke_workflow_report_path", "")),
                batch_workflow_report_path=str(payload.get("batch_workflow_report_path", "")),
                supplemental_backend_smoke_workflow_report_paths=list(supplemental),
                out_root=artifact_root,
                skip_autoware_bridge=bool(payload.get("skip_autoware_bridge", False)),
                autoware_base_frame=str(payload.get("autoware_base_frame", "base_link")),
                autoware_consumer_profile=str(payload.get("autoware_consumer_profile", "")),
                autoware_strict=bool(payload.get("autoware_strict", False)),
                run_history_guard=bool(payload.get("run_history_guard", False)),
                history_guard_metadata_root=payload.get("history_guard_metadata_root"),
                history_guard_current_repo_root=payload.get("history_guard_current_repo_root"),
                history_guard_compare_ref=str(payload.get("history_guard_compare_ref", "origin/main")),
                history_guard_include_untracked=bool(payload.get("history_guard_include_untracked", False)),
            )
        if run_type == "probe_set":
            return run_scenario_runtime_backend_probe_set(
                out_root=artifact_root,
                probe_set_id=str(payload.get("probe_set_id", "hybrid_runtime_readiness_v0")),
                repo_root=payload.get("repo_root"),
                autoware_base_frame=str(payload.get("autoware_base_frame", "base_link")),
                autoware_strict=bool(payload.get("autoware_strict", False)),
                run_history_guard=bool(payload.get("run_history_guard", False)),
                history_guard_metadata_root=payload.get("history_guard_metadata_root"),
                history_guard_current_repo_root=payload.get("history_guard_current_repo_root"),
                history_guard_compare_ref=str(payload.get("history_guard_compare_ref", "origin/main")),
                history_guard_include_untracked=bool(payload.get("history_guard_include_untracked", False)),
            )
        raise ValueError(f"unsupported run_type: {run_type}")

    def _summarize_result(self, run_type: str, result: dict[str, Any]) -> tuple[str, str, str, str, list[str]]:
        summary_json_path = self._first_existing_path(result, [
            "summary_path",
            "workflow_report_path",
            "report_path",
            "probe_set_report_path",
            "manifest_path",
        ])
        summary_markdown_path = self._first_existing_path(result, [
            "workflow_markdown_path",
            "markdown_report_path",
            "report_markdown_path",
            "probe_set_markdown_path",
        ])
        recommended_next_command = self._find_recommended_command(result)
        raw_status = str(
            result.get("status")
            or result.get("workflow_report", {}).get("status")
            or result.get("summary", {}).get("status")
            or ""
        ).strip()
        if not raw_status and summary_json_path:
            try:
                payload = json.loads(Path(summary_json_path).read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                raw_status = str(payload.get("status", "")).strip()
        status = self._canonical_run_status(raw_status or "SUCCEEDED")
        reason_codes = self._collect_reason_codes(result)
        return summary_json_path, summary_markdown_path, recommended_next_command, status, reason_codes

    def _collect_reason_codes(self, result: dict[str, Any]) -> list[str]:
        reason_codes: list[str] = []
        for key in (
            "status_reason_codes",
            "failure_codes",
            "gate_failure_codes",
            "backend_output_comparison_mismatch_reasons",
        ):
            value = result.get(key)
            if isinstance(value, list):
                for item in value:
                    text = str(item).strip()
                    if text and text not in reason_codes:
                        reason_codes.append(text)
        status_summary = result.get("status_summary")
        if isinstance(status_summary, dict):
            for key in (
                "status_reason_codes",
                "gate_failure_codes",
                "backend_output_comparison_mismatch_reasons",
            ):
                value = status_summary.get(key)
                if isinstance(value, list):
                    for item in value:
                        text = str(item).strip()
                        if text and text not in reason_codes:
                            reason_codes.append(text)
        return reason_codes

    def _find_recommended_command(self, payload: Any) -> str:
        if isinstance(payload, dict):
            direct = payload.get("recommended_next_command")
            if str(direct).strip():
                return str(direct).strip()
            for value in payload.values():
                command = self._find_recommended_command(value)
                if command:
                    return command
        elif isinstance(payload, list):
            for value in payload:
                command = self._find_recommended_command(value)
                if command:
                    return command
        return ""

    def _first_existing_path(self, payload: Any, keys: list[str]) -> str:
        matches: list[str] = []
        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, nested in value.items():
                    if key in keys and str(nested).strip():
                        matches.append(str(nested).strip())
                    visit(nested)
            elif isinstance(value, list):
                for nested in value:
                    visit(nested)
        visit(payload)
        for match in matches:
            if Path(match).exists():
                return str(Path(match).resolve())
        return str(Path(matches[0]).resolve()) if matches else ""

    def _index_artifacts(self, artifact_root: Path, result: dict[str, Any]) -> list[dict[str, Any]]:
        seen: dict[str, dict[str, Any]] = {}
        if artifact_root.exists():
            for path in artifact_root.rglob("*"):
                if not path.is_file():
                    continue
                seen[str(path.resolve())] = self._artifact_entry(path, artifact_type=self._infer_artifact_type(path))
        for path_text in self._collect_path_values(result):
            path = Path(path_text)
            if path.exists() and path.is_file():
                seen.setdefault(str(path.resolve()), self._artifact_entry(path, artifact_type=self._infer_artifact_type(path)))
        return sorted(seen.values(), key=lambda item: item["display_name"])

    def _collect_path_values(self, payload: Any) -> list[str]:
        values: list[str] = []
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key.endswith("_path") or key.endswith("_root"):
                    text = str(value).strip()
                    if text:
                        values.append(text)
                values.extend(self._collect_path_values(value))
        elif isinstance(payload, list):
            for value in payload:
                values.extend(self._collect_path_values(value))
        return values

    def _artifact_entry(self, path: Path, *, artifact_type: str) -> dict[str, Any]:
        mime_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        return {
            "artifact_type": artifact_type,
            "path": str(path.resolve()),
            "mime_type": mime_type,
            "display_name": path.name,
            "created_at": utc_now_iso(),
        }

    def _infer_artifact_type(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "json"
        if suffix in {".md", ".markdown"}:
            return "markdown"
        if suffix == ".csv":
            return "csv"
        if suffix in {".txt", ".log", ".sh"}:
            return "text"
        return "binary"

    def _json_safe(self, value: Any) -> Any:
        if isinstance(value, Path):
            return str(value.resolve())
        if isinstance(value, dict):
            return {str(key): self._json_safe(nested) for key, nested in value.items()}
        if isinstance(value, list):
            return [self._json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [self._json_safe(item) for item in value]
        if isinstance(value, set):
            return [self._json_safe(item) for item in sorted(value, key=lambda item: str(item))]
        return value

    def _canonical_run_status(self, status: str) -> str:
        normalized = status.strip()
        if not normalized:
            return "SUCCEEDED"
        lowered = normalized.lower()
        legacy_map = {
            "success": "SUCCEEDED",
            "succeeded": "SUCCEEDED",
            "pass": "READY",
            "ok": "SUCCEEDED",
            "failed": "FAILED",
            "fail": "FAILED",
            "error": "FAILED",
            "running": "RUNNING",
            "planned": "PLANNED",
            "ready": "READY",
            "degraded": "DEGRADED",
            "attention": "ATTENTION",
            "blocked": "BLOCKED",
        }
        return legacy_map.get(lowered, normalized.upper())
