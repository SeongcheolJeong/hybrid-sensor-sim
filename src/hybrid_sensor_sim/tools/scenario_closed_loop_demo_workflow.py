from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCENARIO_CLOSED_LOOP_DEMO_REPORT_SCHEMA_VERSION_V0 = "scenario_closed_loop_demo_report_v0"

_HELPER_SCRIPT_NAMES = {
    "awsim_launch": "launch_awsim_closed_loop.sh",
    "autoware_launch": "launch_autoware_closed_loop.sh",
    "route_goal": "send_route_goal.sh",
    "localization_check": "check_localization_ready.sh",
    "perception_check": "check_perception_ready.sh",
    "planning_check": "check_planning_ready.sh",
    "control_check": "check_control_ready.sh",
    "vehicle_motion_check": "check_vehicle_motion.sh",
    "route_completion_check": "check_route_completed.sh",
    "video_capture": "capture_awsim_video.sh",
    "rviz_capture": "capture_rviz_video.sh",
    "rosbag_record": "record_rosbag.sh",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _load_json_object(path_text: str) -> dict[str, Any] | None:
    text = str(path_text or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _optional_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() == "none":
        return ""
    return text


def _bool_text(value: bool) -> str:
    return "READY" if value else "BLOCKED"


def _quote_command(command: str) -> str:
    return command if command else ""


def _resolve_input_path(path_text: str, *, repo_root: Path | None = None) -> Path:
    candidate = Path(str(path_text).strip()).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    if repo_root is not None:
        return (repo_root / candidate).resolve()
    return candidate.resolve()


def _find_awsim_runtime_binary(awsim_runtime_root: Path) -> str:
    if not awsim_runtime_root.exists() or not awsim_runtime_root.is_dir():
        return ""
    for pattern in ("*.x86_64", "*.AppImage", "*.appimage"):
        matches = sorted(awsim_runtime_root.rglob(pattern))
        if matches:
            return str(matches[0].resolve())
    return ""


def _extract_autoware_bundle_paths(
    *,
    runtime_backend_workflow_report_path: str,
    backend_smoke_workflow_report_path: str,
    explicit_pipeline_manifest_path: str,
    explicit_dataset_manifest_path: str,
    explicit_topic_catalog_path: str,
    explicit_consumer_input_manifest_path: str,
) -> dict[str, str]:
    paths = {
        "autoware_pipeline_manifest_path": _optional_text(explicit_pipeline_manifest_path),
        "autoware_dataset_manifest_path": _optional_text(explicit_dataset_manifest_path),
        "autoware_topic_catalog_path": _optional_text(explicit_topic_catalog_path),
        "autoware_consumer_input_manifest_path": _optional_text(explicit_consumer_input_manifest_path),
    }
    if all(paths.values()):
        return paths

    nested_sources = []
    runtime_payload = _load_json_object(runtime_backend_workflow_report_path)
    if isinstance(runtime_payload, dict):
        nested_sources.extend([
            runtime_payload.get("artifacts", {}),
            runtime_payload.get("backend_smoke_workflow", {}).get("artifacts", {}),
        ])
    backend_payload = _load_json_object(backend_smoke_workflow_report_path)
    if isinstance(backend_payload, dict):
        nested_sources.append(backend_payload.get("artifacts", {}))

    for source in nested_sources:
        if not isinstance(source, dict):
            continue
        for field_name in tuple(paths):
            if not paths[field_name]:
                candidate = _optional_text(source.get(field_name))
                if candidate:
                    paths[field_name] = candidate
    return paths


def _build_autoware_preflight_summary(bundle_paths: dict[str, str]) -> dict[str, Any]:
    pipeline_manifest = _load_json_object(bundle_paths.get("autoware_pipeline_manifest_path", "")) or {}
    dataset_manifest = _load_json_object(bundle_paths.get("autoware_dataset_manifest_path", "")) or {}
    topic_catalog = _load_json_object(bundle_paths.get("autoware_topic_catalog_path", "")) or {}
    consumer_input_manifest = _load_json_object(bundle_paths.get("autoware_consumer_input_manifest_path", "")) or {}
    available_topics = list(topic_catalog.get("available_topics", []) or [])
    missing_required_topics = list(topic_catalog.get("missing_required_topics", []) or [])
    processing_stages = list(consumer_input_manifest.get("processing_stages", []) or [])
    degraded_processing_stage_ids = [
        str(stage.get("stage_id") or "").strip()
        for stage in processing_stages
        if isinstance(stage, dict) and str(stage.get("status") or "").strip().upper() not in {"", "READY"}
    ]
    return {
        "topic_catalog_available": bool(bundle_paths.get("autoware_topic_catalog_path")),
        "consumer_input_manifest_available": bool(bundle_paths.get("autoware_consumer_input_manifest_path")),
        "pipeline_manifest_available": bool(bundle_paths.get("autoware_pipeline_manifest_path")),
        "dataset_manifest_available": bool(bundle_paths.get("autoware_dataset_manifest_path")),
        "consumer_profile": _optional_text(
            pipeline_manifest.get("consumer_profile") or consumer_input_manifest.get("consumer_profile_id")
        ),
        "available_topics": available_topics,
        "missing_required_topics": missing_required_topics,
        "required_topics_complete": not missing_required_topics,
        "available_topic_count": len(available_topics),
        "missing_required_topic_count": len(missing_required_topics),
        "processing_stage_count": len(processing_stages),
        "degraded_processing_stage_ids": [stage_id for stage_id in degraded_processing_stage_ids if stage_id],
        "pipeline_status": _optional_text(pipeline_manifest.get("status")),
        "dataset_ready": dataset_manifest.get("dataset_ready"),
        **bundle_paths,
    }


def _resolve_helper_command(linux_runtime_root: Path, explicit_command: str, helper_key: str) -> dict[str, str]:
    explicit = _optional_text(explicit_command)
    if explicit:
        return {
            "command": explicit,
            "source": "explicit",
            "helper_path": "",
        }
    helper_name = _HELPER_SCRIPT_NAMES[helper_key]
    helper_path = (linux_runtime_root / "bin" / helper_name).resolve()
    if helper_path.exists() and helper_path.is_file():
        return {
            "command": shlex.quote(str(helper_path)),
            "source": "linux_runtime_root.bin",
            "helper_path": str(helper_path),
        }
    return {
        "command": "",
        "source": "missing",
        "helper_path": str(helper_path),
    }


def _run_shell_command(
    command: str,
    *,
    env: dict[str, str],
    cwd: Path,
    timeout_sec: float,
) -> dict[str, Any]:
    if not _optional_text(command):
        return {
            "success": False,
            "returncode": None,
            "stdout": "",
            "stderr": "",
            "timeout": False,
        }
    try:
        completed = subprocess.run(
            ["bash", "-c", command],
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "success": completed.returncode == 0,
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "timeout": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "success": False,
            "returncode": None,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
            "timeout": True,
        }


def _launch_background_process(
    command: str,
    *,
    env: dict[str, str],
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
) -> subprocess.Popen[str]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        ["bash", "-c", command],
        cwd=str(cwd),
        env=env,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    process._codex_stdout_handle = stdout_handle  # type: ignore[attr-defined]
    process._codex_stderr_handle = stderr_handle  # type: ignore[attr-defined]
    return process


def _terminate_process(process: subprocess.Popen[str] | None, *, timeout_sec: float = 10.0) -> dict[str, Any]:
    if process is None:
        return {"terminated": False, "returncode": None}
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=timeout_sec)
        return {"terminated": True, "returncode": process.returncode}
    finally:
        stdout_handle = getattr(process, "_codex_stdout_handle", None)
        stderr_handle = getattr(process, "_codex_stderr_handle", None)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def _command_exists(command_name: str) -> bool:
    return shutil.which(command_name) is not None


def _probe_ros2(autoware_workspace_root: Path, env: dict[str, str], cwd: Path) -> dict[str, Any]:
    setup_path = autoware_workspace_root / "install" / "setup.bash"
    if not setup_path.exists():
        return {
            "ready": False,
            "command": "",
            "source": "missing_setup_bash",
            "stdout": "",
            "stderr": f"missing Autoware setup: {setup_path}",
        }
    check_command = f"source {shlex.quote(str(setup_path))} >/dev/null 2>&1 && command -v ros2 >/dev/null 2>&1 && ros2 --version"
    result = _run_shell_command(check_command, env=env, cwd=cwd, timeout_sec=10.0)
    return {
        "ready": bool(result.get("success")),
        "command": check_command,
        "source": "autoware_workspace.install.setup.bash",
        "stdout": result.get("stdout", ""),
        "stderr": result.get("stderr", ""),
    }


def _collect_capture_outputs(capture_root: Path) -> dict[str, Any]:
    video_paths = [str(path.resolve()) for path in sorted(capture_root.rglob("*.mp4")) if path.is_file()]
    return {
        "video_paths": video_paths,
        "awsim_camera_capture_path": next((path for path in video_paths if path.endswith("awsim_camera_capture.mp4")), ""),
        "rviz_capture_path": next((path for path in video_paths if path.endswith("rviz_capture.mp4")), ""),
        "screen_quad_capture_path": next((path for path in video_paths if path.endswith("screen_quad_capture.mp4")), ""),
    }


def _recommended_next_command(
    *,
    status_reason_codes: list[str],
    command_specs: dict[str, dict[str, str]],
    autoware_workspace_root: Path,
    awsim_runtime_root: Path,
    linux_runtime_root: Path,
    strict_capture: bool,
) -> str:
    codes = {str(code).strip().upper() for code in status_reason_codes if str(code).strip()}
    if "LINUX_RUNTIME_MISSING" in codes:
        return "Run this workflow on a Linux host and re-run the same closed-loop demo command."
    if "ROS2_MISSING" in codes:
        setup_path = autoware_workspace_root / "install" / "setup.bash"
        return f"bash -lc 'source {shlex.quote(str(setup_path))} && ros2 --version'"
    if "AUTOWARE_WORKSPACE_MISSING" in codes:
        return f"Populate {autoware_workspace_root} with an Autoware workspace that includes install/setup.bash."
    if "AWSIM_RUNTIME_MISSING" in codes:
        return f"Populate {awsim_runtime_root} with the AWSIM packaged runtime or provide --awsim-launch-command."
    if "TOPIC_BRIDGE_MISSING" in codes:
        return "Provide Autoware topic catalog and consumer input manifest paths, or pass a runtime/backend workflow report with embedded bundle artifact paths."
    if "CONTROL_LOOP_MISSING" in codes:
        missing_helpers = [
            spec.get("helper_path", "")
            for key, spec in command_specs.items()
            if key in {"route_goal", "localization_check", "perception_check", "planning_check", "control_check", "vehicle_motion_check"}
            and not _optional_text(spec.get("command"))
        ]
        if missing_helpers:
            return f"Create the required helper scripts under {linux_runtime_root / 'bin'} or provide explicit --*-command overrides."
        return "Inspect the closed-loop helper commands and rerun the demo workflow."
    if "VIDEO_CAPTURE_FAILED" in codes:
        if strict_capture:
            return "Provide --video-capture-command or install a capture helper under linux_runtime_root/bin/capture_awsim_video.sh and rerun."
        return "Provide a video capture command or helper script so the next run can emit awsim_camera_capture.mp4."
    for key in ("route_goal", "awsim_launch", "autoware_launch"):
        command = _optional_text(command_specs.get(key, {}).get("command"))
        if command:
            return command
    return "Inspect the closed-loop demo report and follow the recorded blocker details."


def _status_from_summary(
    *,
    blockers: list[str],
    launch_failed: bool,
    awsim_launch_ready: bool,
    autoware_launch_ready: bool,
    localization_ready: bool,
    perception_ready: bool,
    planning_ready: bool,
    control_ready: bool,
    vehicle_motion_confirmed: bool,
    capture_ready: bool,
) -> str:
    if blockers:
        return "BLOCKED"
    if launch_failed:
        return "FAILED"
    required_flags = [
        awsim_launch_ready,
        autoware_launch_ready,
        localization_ready,
        perception_ready,
        planning_ready,
        control_ready,
        vehicle_motion_confirmed,
    ]
    if not all(required_flags) or not capture_ready:
        return "DEGRADED"
    return "SUCCEEDED"


def _build_markdown_report(report: dict[str, Any]) -> str:
    status_summary = dict(report.get("status_summary", {}))
    artifacts = dict(report.get("artifacts", {}))
    preflight = dict(report.get("preflight", {}))
    capture = dict(report.get("capture", {}))
    return "\n".join(
        [
            "# AWSIM Closed-Loop Demo Workflow",
            "",
            f"- Status: `{report.get('status') or '-'}`",
            f"- Reason codes: `{', '.join(report.get('status_reason_codes', [])) or '-'}`",
            f"- Scenario path: `{status_summary.get('scenario_path') or '-'}`",
            f"- Map path: `{status_summary.get('map_path') or '-'}`",
            f"- Route path: `{status_summary.get('route_path') or '-'}`",
            f"- AWSIM launch ready: `{status_summary.get('awsim_launch_ready')}`",
            f"- Autoware launch ready: `{status_summary.get('autoware_launch_ready')}`",
            f"- Localization ready: `{status_summary.get('localization_ready')}`",
            f"- Perception ready: `{status_summary.get('perception_ready')}`",
            f"- Planning ready: `{status_summary.get('planning_ready')}`",
            f"- Control ready: `{status_summary.get('control_ready')}`",
            f"- Vehicle motion confirmed: `{status_summary.get('vehicle_motion_confirmed')}`",
            f"- Route completed: `{status_summary.get('route_completed')}`",
            f"- Capture ready: `{status_summary.get('capture_ready')}`",
            f"- Recommended next command: `{report.get('recommended_next_command') or '-'}`",
            "",
            "## Preflight",
            "",
            f"- Linux host ready: `{preflight.get('linux_host_ready')}`",
            f"- GPU detected: `{preflight.get('gpu_detected')}`",
            f"- ROS2 ready: `{preflight.get('ros2_ready')}`",
            f"- Autoware workspace ready: `{preflight.get('autoware_workspace_ready')}`",
            f"- AWSIM runtime ready: `{preflight.get('awsim_runtime_ready')}`",
            f"- Topic catalog available: `{preflight.get('topic_catalog_available')}`",
            f"- Consumer input manifest available: `{preflight.get('consumer_input_manifest_available')}`",
            f"- Missing required topics: `{', '.join(preflight.get('missing_required_topics', [])) or '-'}`",
            "",
            "## Capture",
            "",
            f"- Requested video: `{capture.get('record_video')}`",
            f"- Requested RViz: `{capture.get('record_rviz')}`",
            f"- Requested rosbag: `{capture.get('record_rosbag')}`",
            f"- Video paths: `{', '.join(capture.get('video_paths', [])) or '-'}`",
            f"- Rosbag path: `{capture.get('rosbag_path') or '-'}`",
            "",
            "## Artifacts",
            "",
            f"- Report: `{artifacts.get('report_path') or '-'}`",
            f"- Markdown: `{artifacts.get('markdown_report_path') or '-'}`",
            f"- Telemetry: `{artifacts.get('telemetry_path') or '-'}`",
            f"- Log root: `{artifacts.get('log_root') or '-'}`",
            f"- Capture root: `{artifacts.get('capture_root') or '-'}`",
            f"- Autoware topic catalog: `{artifacts.get('autoware_topic_catalog_path') or '-'}`",
            f"- Autoware consumer input manifest: `{artifacts.get('autoware_consumer_input_manifest_path') or '-'}`",
        ]
    )


def run_scenario_closed_loop_demo(
    *,
    scenario_path: str,
    linux_runtime_root: str,
    autoware_workspace_root: str,
    awsim_runtime_root: str,
    map_path: str,
    route_path: str,
    out_root: Path,
    runtime_backend_workflow_report_path: str = "",
    backend_smoke_workflow_report_path: str = "",
    autoware_pipeline_manifest_path: str = "",
    autoware_dataset_manifest_path: str = "",
    autoware_topic_catalog_path: str = "",
    autoware_consumer_input_manifest_path: str = "",
    run_duration_sec: float = 45.0,
    heartbeat_timeout_sec: float = 60.0,
    poll_interval_sec: float = 2.0,
    startup_grace_sec: float = 5.0,
    record_video: bool = True,
    record_rviz: bool = False,
    record_rosbag: bool = False,
    strict_capture: bool = False,
    preflight_only: bool = False,
    allow_non_linux_host: bool = False,
    awsim_launch_command: str = "",
    autoware_launch_command: str = "",
    route_goal_command: str = "",
    localization_check_command: str = "",
    perception_check_command: str = "",
    planning_check_command: str = "",
    control_check_command: str = "",
    vehicle_motion_check_command: str = "",
    route_completion_check_command: str = "",
    video_capture_command: str = "",
    rviz_capture_command: str = "",
    rosbag_record_command: str = "",
) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[3]
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    log_root = out_root / "logs"
    capture_root = out_root / "capture"
    rosbag_root = out_root / "rosbag"
    telemetry_path = out_root / "run_telemetry.json"
    report_path = out_root / "scenario_closed_loop_demo_report_v0.json"
    markdown_path = out_root / "scenario_closed_loop_demo_report_v0.md"
    log_root.mkdir(parents=True, exist_ok=True)
    capture_root.mkdir(parents=True, exist_ok=True)
    rosbag_root.mkdir(parents=True, exist_ok=True)

    scenario_file = _resolve_input_path(scenario_path, repo_root=repo_root)
    linux_root = _resolve_input_path(linux_runtime_root, repo_root=repo_root)
    autoware_root = _resolve_input_path(autoware_workspace_root, repo_root=repo_root)
    awsim_root = _resolve_input_path(awsim_runtime_root, repo_root=repo_root)
    map_file = _resolve_input_path(map_path, repo_root=repo_root)
    route_file = _resolve_input_path(route_path, repo_root=repo_root)

    timeline: list[dict[str, Any]] = []

    def add_event(event: str, **fields: Any) -> None:
        timeline.append({"time_utc": _utc_now(), "event": event, **fields})

    add_event("workflow_started", out_root=str(out_root))

    bundle_paths = _extract_autoware_bundle_paths(
        runtime_backend_workflow_report_path=runtime_backend_workflow_report_path,
        backend_smoke_workflow_report_path=backend_smoke_workflow_report_path,
        explicit_pipeline_manifest_path=autoware_pipeline_manifest_path,
        explicit_dataset_manifest_path=autoware_dataset_manifest_path,
        explicit_topic_catalog_path=autoware_topic_catalog_path,
        explicit_consumer_input_manifest_path=autoware_consumer_input_manifest_path,
    )
    autoware_preflight = _build_autoware_preflight_summary(bundle_paths)

    base_env = os.environ.copy()
    base_env.update(
        {
            "LINUX_RUNTIME_ROOT": str(linux_root),
            "AUTOWARE_WORKSPACE_ROOT": str(autoware_root),
            "AWSIM_RUNTIME_ROOT": str(awsim_root),
            "SCENARIO_PATH": str(scenario_file),
            "MAP_PATH": str(map_file),
            "ROUTE_PATH": str(route_file),
            "RUN_OUT_ROOT": str(out_root),
            "CAPTURE_ROOT": str(capture_root),
            "ROSBAG_ROOT": str(rosbag_root),
            "AWSIM_CAMERA_CAPTURE_PATH": str(capture_root / "awsim_camera_capture.mp4"),
            "RVIZ_CAPTURE_PATH": str(capture_root / "rviz_capture.mp4"),
            "SCREEN_QUAD_CAPTURE_PATH": str(capture_root / "screen_quad_capture.mp4"),
            "AUTOWARE_TOPIC_CATALOG_PATH": bundle_paths.get("autoware_topic_catalog_path", ""),
            "AUTOWARE_CONSUMER_INPUT_MANIFEST_PATH": bundle_paths.get("autoware_consumer_input_manifest_path", ""),
            "AUTOWARE_PIPELINE_MANIFEST_PATH": bundle_paths.get("autoware_pipeline_manifest_path", ""),
            "AUTOWARE_DATASET_MANIFEST_PATH": bundle_paths.get("autoware_dataset_manifest_path", ""),
        }
    )

    command_specs = {
        "awsim_launch": _resolve_helper_command(linux_root, awsim_launch_command, "awsim_launch"),
        "autoware_launch": _resolve_helper_command(linux_root, autoware_launch_command, "autoware_launch"),
        "route_goal": _resolve_helper_command(linux_root, route_goal_command, "route_goal"),
        "localization_check": _resolve_helper_command(linux_root, localization_check_command, "localization_check"),
        "perception_check": _resolve_helper_command(linux_root, perception_check_command, "perception_check"),
        "planning_check": _resolve_helper_command(linux_root, planning_check_command, "planning_check"),
        "control_check": _resolve_helper_command(linux_root, control_check_command, "control_check"),
        "vehicle_motion_check": _resolve_helper_command(linux_root, vehicle_motion_check_command, "vehicle_motion_check"),
        "route_completion_check": _resolve_helper_command(linux_root, route_completion_check_command, "route_completion_check"),
        "video_capture": _resolve_helper_command(linux_root, video_capture_command, "video_capture"),
        "rviz_capture": _resolve_helper_command(linux_root, rviz_capture_command, "rviz_capture"),
        "rosbag_record": _resolve_helper_command(linux_root, rosbag_record_command, "rosbag_record"),
    }

    missing_inputs: list[str] = []
    for label, path in (
        ("scenario", scenario_file),
        ("map", map_file),
        ("route", route_file),
    ):
        if not path.exists() or not path.is_file():
            missing_inputs.append(f"{label}:{path}")
    linux_host_ready = sys.platform.startswith("linux") or allow_non_linux_host
    gpu_detected = _command_exists("nvidia-smi")
    autoware_workspace_ready = autoware_root.exists() and autoware_root.is_dir() and (autoware_root / "install" / "setup.bash").exists()
    ros2_probe = _probe_ros2(autoware_root, base_env, linux_root if linux_root.exists() else repo_root) if autoware_workspace_ready else {
        "ready": False,
        "command": "",
        "source": "workspace_missing",
        "stdout": "",
        "stderr": "Autoware workspace missing or install/setup.bash not found.",
    }
    awsim_runtime_binary = _find_awsim_runtime_binary(awsim_root)
    awsim_runtime_ready = awsim_root.exists() and awsim_root.is_dir() and (bool(awsim_runtime_binary) or bool(_optional_text(command_specs["awsim_launch"]["command"])))
    topic_bridge_ready = bool(autoware_preflight.get("topic_catalog_available")) and bool(autoware_preflight.get("consumer_input_manifest_available")) and not list(autoware_preflight.get("missing_required_topics", []))

    blocker_codes: list[str] = []
    if missing_inputs:
        blocker_codes.append("MAP_OR_ROUTE_MISSING")
    if not linux_root.exists() or not linux_root.is_dir() or not linux_host_ready:
        blocker_codes.append("LINUX_RUNTIME_MISSING")
    if not autoware_workspace_ready:
        blocker_codes.append("AUTOWARE_WORKSPACE_MISSING")
    if not bool(ros2_probe.get("ready")):
        blocker_codes.append("ROS2_MISSING")
    if not awsim_runtime_ready:
        blocker_codes.append("AWSIM_RUNTIME_MISSING")
    if not topic_bridge_ready:
        blocker_codes.append("TOPIC_BRIDGE_MISSING")

    required_control_commands = [
        "route_goal",
        "localization_check",
        "perception_check",
        "planning_check",
        "control_check",
        "vehicle_motion_check",
    ]
    missing_control_commands = [
        key for key in required_control_commands if not _optional_text(command_specs[key]["command"])
    ]
    if missing_control_commands and not preflight_only:
        blocker_codes.append("CONTROL_LOOP_MISSING")

    capture_blocker = False
    if record_video and not _optional_text(command_specs["video_capture"]["command"]):
        capture_blocker = True
        if strict_capture:
            blocker_codes.append("VIDEO_CAPTURE_FAILED")
    if record_rviz and not _optional_text(command_specs["rviz_capture"]["command"]):
        capture_blocker = True
        if strict_capture:
            blocker_codes.append("VIDEO_CAPTURE_FAILED")
    if record_rosbag and not _optional_text(command_specs["rosbag_record"]["command"]):
        capture_blocker = True
        if strict_capture:
            blocker_codes.append("VIDEO_CAPTURE_FAILED")

    blocker_codes = sorted(set(blocker_codes))
    add_event("preflight_completed", blocker_codes=blocker_codes)

    processes: dict[str, subprocess.Popen[str] | None] = {
        "awsim": None,
        "autoware": None,
        "video": None,
        "rviz": None,
        "rosbag": None,
    }
    launch_failed = False
    awsim_launch_ready = False
    autoware_launch_ready = False
    localization_ready = False
    perception_ready = False
    planning_ready = False
    control_ready = False
    vehicle_motion_confirmed = False
    route_completed = False
    capture_ready = not record_video and not record_rviz and not record_rosbag
    capture_started_at = ""
    capture_finished_at = ""
    launch_notes: list[str] = []

    try:
        if not blocker_codes and not preflight_only:
            cwd = linux_root if linux_root.exists() else repo_root
            add_event("launching_awsim", command=command_specs["awsim_launch"]["command"])
            processes["awsim"] = _launch_background_process(
                command_specs["awsim_launch"]["command"],
                env=base_env,
                cwd=cwd,
                stdout_path=log_root / "awsim.stdout.log",
                stderr_path=log_root / "awsim.stderr.log",
            )
            time.sleep(max(startup_grace_sec, 0.1))
            if processes["awsim"] is not None and processes["awsim"].poll() is None:
                awsim_launch_ready = True
            else:
                launch_failed = True
                launch_notes.append("AWSIM process exited during startup grace period.")
                add_event("awsim_launch_failed", returncode=processes["awsim"].returncode if processes["awsim"] is not None else None)

            if not launch_failed:
                add_event("launching_autoware", command=command_specs["autoware_launch"]["command"])
                processes["autoware"] = _launch_background_process(
                    command_specs["autoware_launch"]["command"],
                    env=base_env,
                    cwd=cwd,
                    stdout_path=log_root / "autoware.stdout.log",
                    stderr_path=log_root / "autoware.stderr.log",
                )
                time.sleep(max(startup_grace_sec, 0.1))
                if processes["autoware"] is not None and processes["autoware"].poll() is None:
                    autoware_launch_ready = True
                else:
                    launch_failed = True
                    launch_notes.append("Autoware process exited during startup grace period.")
                    add_event("autoware_launch_failed", returncode=processes["autoware"].returncode if processes["autoware"] is not None else None)

            if not launch_failed:
                if record_video and _optional_text(command_specs["video_capture"]["command"]):
                    capture_started_at = _utc_now()
                    processes["video"] = _launch_background_process(
                        command_specs["video_capture"]["command"],
                        env=base_env,
                        cwd=cwd,
                        stdout_path=log_root / "video_capture.stdout.log",
                        stderr_path=log_root / "video_capture.stderr.log",
                    )
                if record_rviz and _optional_text(command_specs["rviz_capture"]["command"]):
                    capture_started_at = capture_started_at or _utc_now()
                    processes["rviz"] = _launch_background_process(
                        command_specs["rviz_capture"]["command"],
                        env=base_env,
                        cwd=cwd,
                        stdout_path=log_root / "rviz_capture.stdout.log",
                        stderr_path=log_root / "rviz_capture.stderr.log",
                    )
                if record_rosbag and _optional_text(command_specs["rosbag_record"]["command"]):
                    processes["rosbag"] = _launch_background_process(
                        command_specs["rosbag_record"]["command"],
                        env=base_env,
                        cwd=cwd,
                        stdout_path=log_root / "rosbag.stdout.log",
                        stderr_path=log_root / "rosbag.stderr.log",
                    )
                capture_ready = not capture_blocker or not strict_capture
                if record_video and not _optional_text(command_specs["video_capture"]["command"]):
                    capture_ready = False
                if record_rviz and not _optional_text(command_specs["rviz_capture"]["command"]):
                    capture_ready = False
                if record_rosbag and not _optional_text(command_specs["rosbag_record"]["command"]):
                    capture_ready = False

            if not launch_failed:
                route_goal_result = _run_shell_command(
                    command_specs["route_goal"]["command"],
                    env=base_env,
                    cwd=cwd,
                    timeout_sec=max(heartbeat_timeout_sec, 5.0),
                )
                add_event("route_goal_command_finished", success=route_goal_result["success"], returncode=route_goal_result["returncode"])
                if not route_goal_result["success"]:
                    launch_failed = True
                    launch_notes.append("Route goal command failed.")

            deadline = time.time() + max(heartbeat_timeout_sec, poll_interval_sec)
            check_specs = {
                "localization_ready": command_specs["localization_check"],
                "perception_ready": command_specs["perception_check"],
                "planning_ready": command_specs["planning_check"],
                "control_ready": command_specs["control_check"],
                "vehicle_motion_confirmed": command_specs["vehicle_motion_check"],
            }
            check_values = {
                "localization_ready": False,
                "perception_ready": False,
                "planning_ready": False,
                "control_ready": False,
                "vehicle_motion_confirmed": False,
            }
            while not launch_failed and time.time() < deadline:
                for field_name, command_spec in check_specs.items():
                    if check_values[field_name]:
                        continue
                    result = _run_shell_command(
                        command_spec["command"],
                        env=base_env,
                        cwd=cwd,
                        timeout_sec=max(poll_interval_sec, 1.0),
                    )
                    if result["success"]:
                        check_values[field_name] = True
                        add_event("heartbeat_ready", field=field_name)
                if all(check_values.values()):
                    break
                time.sleep(max(poll_interval_sec, 0.1))
            localization_ready = check_values["localization_ready"]
            perception_ready = check_values["perception_ready"]
            planning_ready = check_values["planning_ready"]
            control_ready = check_values["control_ready"]
            vehicle_motion_confirmed = check_values["vehicle_motion_confirmed"]

            if not launch_failed:
                run_deadline = time.time() + max(run_duration_sec, 0.1)
                route_completion_command = _optional_text(command_specs["route_completion_check"]["command"])
                while time.time() < run_deadline:
                    if route_completion_command:
                        completion_result = _run_shell_command(
                            route_completion_command,
                            env=base_env,
                            cwd=cwd,
                            timeout_sec=max(poll_interval_sec, 1.0),
                        )
                        if completion_result["success"]:
                            route_completed = True
                            add_event("route_completed")
                            break
                    time.sleep(max(poll_interval_sec, 0.1))
    finally:
        for process_name in ("video", "rviz", "rosbag", "autoware", "awsim"):
            termination = _terminate_process(processes[process_name])
            if termination["terminated"]:
                add_event("process_terminated", process=process_name, returncode=termination["returncode"])
        capture_finished_at = _utc_now() if capture_started_at else ""

    capture_outputs = _collect_capture_outputs(capture_root)
    video_paths = list(capture_outputs.get("video_paths", []))
    rosbag_path = str(rosbag_root.resolve()) if any(rosbag_root.iterdir()) else (str(rosbag_root.resolve()) if record_rosbag and rosbag_root.exists() else "")
    if record_video and not capture_outputs.get("awsim_camera_capture_path"):
        capture_ready = False
        if "VIDEO_CAPTURE_FAILED" not in blocker_codes:
            blocker_codes.append("VIDEO_CAPTURE_FAILED")
    if record_rviz and not capture_outputs.get("rviz_capture_path") and strict_capture:
        capture_ready = False
        if "VIDEO_CAPTURE_FAILED" not in blocker_codes:
            blocker_codes.append("VIDEO_CAPTURE_FAILED")
    if record_rosbag and not rosbag_path and strict_capture:
        capture_ready = False
        if "VIDEO_CAPTURE_FAILED" not in blocker_codes:
            blocker_codes.append("VIDEO_CAPTURE_FAILED")

    blocker_codes = sorted(set(blocker_codes))
    final_status = _status_from_summary(
        blockers=[code for code in blocker_codes if code not in {"VIDEO_CAPTURE_FAILED"} or strict_capture],
        launch_failed=launch_failed,
        awsim_launch_ready=awsim_launch_ready,
        autoware_launch_ready=autoware_launch_ready,
        localization_ready=localization_ready,
        perception_ready=perception_ready,
        planning_ready=planning_ready,
        control_ready=control_ready,
        vehicle_motion_confirmed=vehicle_motion_confirmed,
        capture_ready=capture_ready,
    )
    if final_status == "BLOCKED" and launch_failed:
        final_status = "FAILED"
    if preflight_only and not blocker_codes:
        final_status = "PLANNED"
    if final_status == "SUCCEEDED" and record_video and not capture_ready:
        final_status = "DEGRADED"

    recommended_next_command = _recommended_next_command(
        status_reason_codes=blocker_codes,
        command_specs=command_specs,
        autoware_workspace_root=autoware_root,
        awsim_runtime_root=awsim_root,
        linux_runtime_root=linux_root,
        strict_capture=strict_capture,
    )

    status_summary = {
        "scenario_path": str(scenario_file),
        "map_path": str(map_file),
        "route_path": str(route_file),
        "linux_runtime_root": str(linux_root),
        "autoware_workspace_root": str(autoware_root),
        "awsim_runtime_root": str(awsim_root),
        "preflight_only": bool(preflight_only),
        "awsim_launch_ready": bool(awsim_launch_ready),
        "autoware_launch_ready": bool(autoware_launch_ready),
        "localization_ready": bool(localization_ready),
        "perception_ready": bool(perception_ready),
        "planning_ready": bool(planning_ready),
        "control_ready": bool(control_ready),
        "vehicle_motion_confirmed": bool(vehicle_motion_confirmed),
        "route_completed": bool(route_completed),
        "capture_ready": bool(capture_ready),
        "video_path_count": len(video_paths),
        "rosbag_available": bool(rosbag_path),
        "consumer_profile": autoware_preflight.get("consumer_profile", ""),
        "missing_required_topic_count": autoware_preflight.get("missing_required_topic_count", 0),
        "missing_required_topics": list(autoware_preflight.get("missing_required_topics", [])),
        "available_topic_count": autoware_preflight.get("available_topic_count", 0),
        "degraded_processing_stage_ids": list(autoware_preflight.get("degraded_processing_stage_ids", [])),
        "status_reason_codes": blocker_codes,
    }
    preflight_summary = {
        "linux_host_ready": bool(linux_host_ready),
        "allow_non_linux_host": bool(allow_non_linux_host),
        "gpu_detected": bool(gpu_detected),
        "ros2_ready": bool(ros2_probe.get("ready")),
        "ros2_probe": ros2_probe,
        "autoware_workspace_ready": bool(autoware_workspace_ready),
        "awsim_runtime_ready": bool(awsim_runtime_ready),
        "awsim_runtime_binary": awsim_runtime_binary,
        "topic_bridge_ready": bool(topic_bridge_ready),
        "missing_inputs": missing_inputs,
        **autoware_preflight,
    }
    capture_summary = {
        "record_video": bool(record_video),
        "record_rviz": bool(record_rviz),
        "record_rosbag": bool(record_rosbag),
        "strict_capture": bool(strict_capture),
        "capture_started_at": capture_started_at,
        "capture_finished_at": capture_finished_at,
        "capture_duration_sec": max(0.0, (datetime.fromisoformat(capture_finished_at.replace("Z", "+00:00")) - datetime.fromisoformat(capture_started_at.replace("Z", "+00:00"))).total_seconds()) if capture_started_at and capture_finished_at else 0.0,
        **capture_outputs,
        "rosbag_path": rosbag_path,
    }
    runtime_commands = {
        key: {
            "command": _optional_text(spec.get("command")),
            "source": spec.get("source", ""),
            "helper_path": spec.get("helper_path", ""),
        }
        for key, spec in command_specs.items()
    }
    artifacts = {
        "report_path": str(report_path),
        "markdown_report_path": str(markdown_path),
        "telemetry_path": str(telemetry_path),
        "log_root": str(log_root.resolve()),
        "capture_root": str(capture_root.resolve()),
        "rosbag_root": str(rosbag_root.resolve()),
        **bundle_paths,
    }
    report = {
        "scenario_closed_loop_demo_report_schema_version": SCENARIO_CLOSED_LOOP_DEMO_REPORT_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "status": final_status,
        "status_reason_codes": blocker_codes,
        "recommended_next_command": recommended_next_command,
        "status_summary": status_summary,
        "preflight": preflight_summary,
        "runtime_commands": runtime_commands,
        "capture": capture_summary,
        "autoware": {
            "consumer_profile": autoware_preflight.get("consumer_profile", ""),
            "available_topics": list(autoware_preflight.get("available_topics", [])),
            "missing_required_topics": list(autoware_preflight.get("missing_required_topics", [])),
            "degraded_processing_stage_ids": list(autoware_preflight.get("degraded_processing_stage_ids", [])),
            **bundle_paths,
        },
        "artifacts": artifacts,
        "launch_notes": launch_notes,
        "timeline": timeline,
    }
    telemetry_payload = {
        "schema_version": "run_telemetry_v0",
        "generated_at_utc": _utc_now(),
        "status": final_status,
        "timeline": timeline,
        "status_summary": status_summary,
        "launch_notes": launch_notes,
    }
    _write_json(telemetry_path, telemetry_payload)
    _write_json(report_path, report)
    _write_text(markdown_path, _build_markdown_report(report))
    return {
        "status": final_status,
        "status_reason_codes": blocker_codes,
        "recommended_next_command": recommended_next_command,
        "report": report,
        "report_path": str(report_path),
        "markdown_report_path": str(markdown_path),
        "telemetry_path": str(telemetry_path),
        **bundle_paths,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run an AWSIM + Autoware closed-loop demo orchestration workflow on a Linux host."
        )
    )
    parser.add_argument("--scenario-path", required=True, help="Scenario JSON used for demo lineage and validation")
    parser.add_argument("--linux-runtime-root", required=True, help="Linux runtime root that contains helper scripts under bin/")
    parser.add_argument("--autoware-workspace-root", required=True, help="Autoware workspace root containing install/setup.bash")
    parser.add_argument("--awsim-runtime-root", required=True, help="AWSIM packaged runtime root")
    parser.add_argument("--map-path", required=True, help="Map input path")
    parser.add_argument("--route-path", required=True, help="Route input path")
    parser.add_argument("--out-root", required=True, help="Output root")
    parser.add_argument("--runtime-backend-workflow-report", default="", help="Optional runtime backend workflow report for Autoware preflight paths")
    parser.add_argument("--backend-smoke-workflow-report", default="", help="Optional backend smoke workflow report for Autoware preflight paths")
    parser.add_argument("--autoware-pipeline-manifest", default="", help="Optional explicit autoware_pipeline_manifest.json path")
    parser.add_argument("--autoware-dataset-manifest", default="", help="Optional explicit autoware_dataset_manifest.json path")
    parser.add_argument("--autoware-topic-catalog", default="", help="Optional explicit autoware_topic_catalog.json path")
    parser.add_argument("--autoware-consumer-input-manifest", default="", help="Optional explicit autoware_consumer_input_manifest.json path")
    parser.add_argument("--run-duration-sec", type=float, default=45.0)
    parser.add_argument("--heartbeat-timeout-sec", type=float, default=60.0)
    parser.add_argument("--poll-interval-sec", type=float, default=2.0)
    parser.add_argument("--startup-grace-sec", type=float, default=5.0)
    parser.add_argument(
        "--record-video",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Record the AWSIM camera capture",
    )
    parser.add_argument(
        "--record-rviz",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Record an RViz overlay capture",
    )
    parser.add_argument(
        "--record-rosbag",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Record rosbag output",
    )
    parser.add_argument("--strict-capture", action="store_true", help="Treat capture setup failures as blockers")
    parser.add_argument("--preflight-only", action="store_true", help="Run readiness checks only without launching runtime processes")
    parser.add_argument("--allow-non-linux-host", action="store_true", help="Bypass the host platform guard for local testing")
    parser.add_argument("--awsim-launch-command", default="", help="Explicit AWSIM launch command override")
    parser.add_argument("--autoware-launch-command", default="", help="Explicit Autoware launch command override")
    parser.add_argument("--route-goal-command", default="", help="Explicit route goal command override")
    parser.add_argument("--localization-check-command", default="", help="Explicit localization readiness check command override")
    parser.add_argument("--perception-check-command", default="", help="Explicit perception readiness check command override")
    parser.add_argument("--planning-check-command", default="", help="Explicit planning readiness check command override")
    parser.add_argument("--control-check-command", default="", help="Explicit control readiness check command override")
    parser.add_argument("--vehicle-motion-check-command", default="", help="Explicit vehicle motion check command override")
    parser.add_argument("--route-completion-check-command", default="", help="Explicit route completion command override")
    parser.add_argument("--video-capture-command", default="", help="Explicit AWSIM video capture command override")
    parser.add_argument("--rviz-capture-command", default="", help="Explicit RViz capture command override")
    parser.add_argument("--rosbag-record-command", default="", help="Explicit rosbag record command override")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        result = run_scenario_closed_loop_demo(
            scenario_path=args.scenario_path,
            linux_runtime_root=args.linux_runtime_root,
            autoware_workspace_root=args.autoware_workspace_root,
            awsim_runtime_root=args.awsim_runtime_root,
            map_path=args.map_path,
            route_path=args.route_path,
            out_root=Path(args.out_root),
            runtime_backend_workflow_report_path=args.runtime_backend_workflow_report,
            backend_smoke_workflow_report_path=args.backend_smoke_workflow_report,
            autoware_pipeline_manifest_path=args.autoware_pipeline_manifest,
            autoware_dataset_manifest_path=args.autoware_dataset_manifest,
            autoware_topic_catalog_path=args.autoware_topic_catalog,
            autoware_consumer_input_manifest_path=args.autoware_consumer_input_manifest,
            run_duration_sec=float(args.run_duration_sec),
            heartbeat_timeout_sec=float(args.heartbeat_timeout_sec),
            poll_interval_sec=float(args.poll_interval_sec),
            startup_grace_sec=float(args.startup_grace_sec),
            record_video=bool(args.record_video),
            record_rviz=bool(args.record_rviz),
            record_rosbag=bool(args.record_rosbag),
            strict_capture=bool(args.strict_capture),
            preflight_only=bool(args.preflight_only),
            allow_non_linux_host=bool(args.allow_non_linux_host),
            awsim_launch_command=args.awsim_launch_command,
            autoware_launch_command=args.autoware_launch_command,
            route_goal_command=args.route_goal_command,
            localization_check_command=args.localization_check_command,
            perception_check_command=args.perception_check_command,
            planning_check_command=args.planning_check_command,
            control_check_command=args.control_check_command,
            vehicle_motion_check_command=args.vehicle_motion_check_command,
            route_completion_check_command=args.route_completion_check_command,
            video_capture_command=args.video_capture_command,
            rviz_capture_command=args.rviz_capture_command,
            rosbag_record_command=args.rosbag_record_command,
        )
        print(f"[ok] status={result['status']}")
        print(f"[ok] report={result['report_path']}")
        return 0 if result["status"] in {"SUCCEEDED", "DEGRADED", "PLANNED"} else 2
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_closed_loop_demo_workflow.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
