from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest, SensorSimResult

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run AWSIM/CARLA renderer smoke execution with automatic output inspection."
    )
    parser.add_argument("--config", type=Path, required=True, help="Base JSON config file.")
    parser.add_argument(
        "--backend",
        choices=("awsim", "carla"),
        required=True,
        help="Renderer backend to smoke.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Override output directory for the smoke run.",
    )
    parser.add_argument(
        "--backend-bin",
        help="Override direct backend executable path for the selected backend.",
    )
    parser.add_argument(
        "--renderer-map",
        help="Override renderer map/town value for the smoke run.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Where to write renderer_backend_smoke_summary.json. Defaults under output_dir.",
    )
    parser.add_argument(
        "--markdown-report-path",
        type=Path,
        help="Where to write renderer_backend_smoke_report.md. Defaults under output_dir.",
    )
    parser.add_argument(
        "--html-report-path",
        type=Path,
        help="Where to write renderer_backend_smoke_report.html. Defaults under output_dir.",
    )
    parser.add_argument(
        "--set-option",
        action="append",
        default=[],
        help="Override option values using dotted keys, for example camera_projection_enabled=true or camera_intrinsics.fx=1200.",
    )
    return parser.parse_args(argv)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Config payload must be a JSON object.")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _resolve_runtime_path(raw: Any) -> Path:
    path = Path(str(raw)).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _expand_env_string(raw: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        env_name = match.group(1)
        default_value = match.group(2)
        env_value = None
        try:
            from os import environ

            env_value = environ.get(env_name)
        except Exception:
            env_value = None
        if env_value is not None:
            return env_value
        if default_value is not None:
            return default_value
        raise ValueError(f"Missing required environment variable: {env_name}")

    if "${" not in raw:
        return raw
    return _ENV_PATTERN.sub(_replace, raw)


def _resolve_env_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {str(key): _resolve_env_payload(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_resolve_env_payload(item) for item in payload]
    if isinstance(payload, str):
        return _expand_env_string(payload)
    return payload


def _parse_option_override(raw: str) -> tuple[str, Any]:
    if "=" not in raw:
        raise ValueError(f"Invalid --set-option value: {raw}")
    key, value_text = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"Invalid --set-option value: {raw}")
    try:
        value = json.loads(value_text)
    except json.JSONDecodeError:
        value = value_text
    return key, value


def _set_dotted_option(options: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [part.strip() for part in dotted_key.split(".") if part.strip()]
    if not parts:
        raise ValueError(f"Invalid option key: {dotted_key}")
    cursor = options
    for part in parts[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            child = {}
            cursor[part] = child
        cursor = child
    cursor[parts[-1]] = value


def _load_artifact_payload(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _build_comparison_table(
    output_comparison_report: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(output_comparison_report, dict):
        return None
    raw_by_sensor = output_comparison_report.get("by_sensor")
    if not isinstance(raw_by_sensor, list):
        return None

    sensor_rows: list[dict[str, Any]] = []
    role_rows: list[dict[str, Any]] = []
    sensor_status_counts: dict[str, int] = {}
    role_status_counts: dict[str, int] = {}
    mismatch_reason_counts: dict[str, int] = {}
    for sensor_entry in raw_by_sensor:
        if not isinstance(sensor_entry, dict):
            continue
        sensor_status = str(sensor_entry.get("status", "")).strip()
        if sensor_status:
            sensor_status_counts[sensor_status] = sensor_status_counts.get(sensor_status, 0) + 1
        sensor_mismatch_reasons = [
            str(item).strip()
            for item in sensor_entry.get("mismatch_reasons", [])
            if str(item).strip()
        ]
        for reason in sensor_mismatch_reasons:
            mismatch_reason_counts[reason] = mismatch_reason_counts.get(reason, 0) + 1
        sensor_rows.append(
            {
                "sensor_id": str(sensor_entry.get("sensor_id", "")).strip(),
                "sensor_name": str(sensor_entry.get("sensor_name", "")).strip(),
                "status": sensor_status,
                "mismatch_reasons": sensor_mismatch_reasons,
                "found_output_roles": [
                    str(item).strip()
                    for item in sensor_entry.get("found_output_roles", [])
                    if str(item).strip()
                ],
                "missing_output_roles": [
                    str(item).strip()
                    for item in sensor_entry.get("missing_output_roles", [])
                    if str(item).strip()
                ],
                "found_output_count": sensor_entry.get("found_output_count"),
                "missing_output_count": sensor_entry.get("missing_output_count"),
                "unexpected_output_count": sensor_entry.get("unexpected_output_count"),
            }
        )
        raw_role_diffs = sensor_entry.get("role_diffs")
        if not isinstance(raw_role_diffs, list):
            continue
        sensor_id = str(sensor_entry.get("sensor_id", "")).strip()
        for role_entry in raw_role_diffs:
            if not isinstance(role_entry, dict):
                continue
            role_status = str(role_entry.get("status", "")).strip()
            if role_status:
                role_status_counts[role_status] = role_status_counts.get(role_status, 0) + 1
            role_mismatch_reasons = [
                str(item).strip()
                for item in role_entry.get("mismatch_reasons", [])
                if str(item).strip()
            ]
            for reason in role_mismatch_reasons:
                mismatch_reason_counts[reason] = mismatch_reason_counts.get(reason, 0) + 1
            role_rows.append(
                {
                    "sensor_id": sensor_id,
                    "output_role": str(role_entry.get("output_role", "")).strip(),
                    "status": role_status,
                    "mismatch_reasons": role_mismatch_reasons,
                    "found_output_count": role_entry.get("found_output_count"),
                    "missing_output_count": role_entry.get("missing_output_count"),
                    "expected_backend_filenames": [
                        str(item).strip()
                        for item in role_entry.get("expected_backend_filenames", [])
                        if str(item).strip()
                    ],
                    "discovered_backend_filenames": [
                        str(item).strip()
                        for item in role_entry.get("discovered_backend_filenames", [])
                        if str(item).strip()
                    ],
                    "found_relative_paths": [
                        str(item).strip()
                        for item in role_entry.get("found_relative_paths", [])
                        if str(item).strip()
                    ],
                    "missing_relative_paths": [
                        str(item).strip()
                        for item in role_entry.get("missing_relative_paths", [])
                        if str(item).strip()
                    ],
                }
            )
    return {
        "sensor_rows": sensor_rows,
        "role_rows": role_rows,
        "sensor_status_counts": sensor_status_counts,
        "role_status_counts": role_status_counts,
        "mismatch_reason_counts": mismatch_reason_counts,
    }


def _markdown_inline(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).strip()
    if not text:
        return "-"
    return text.replace("\n", " ")


def _markdown_list(value: Any) -> str:
    if isinstance(value, list):
        items = [_markdown_inline(item) for item in value if _markdown_inline(item) != "-"]
        return ", ".join(items) if items else "-"
    return _markdown_inline(value)


def _markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    table_lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        table_lines.append("| " + " | ".join(_markdown_inline(cell) for cell in row) + " |")
    return "\n".join(table_lines)


def _render_markdown_report(summary: dict[str, Any], summary_path: Path) -> str:
    lines = [
        "# Renderer Backend Smoke Report",
        "",
        "## Overview",
        f"- backend: `{_markdown_inline(summary.get('backend'))}`",
        f"- success: `{_markdown_inline(summary.get('success'))}`",
        f"- run status: `{_markdown_inline(summary.get('run', {}).get('status') if isinstance(summary.get('run'), dict) else None)}`",
        f"- failure reason: `{_markdown_inline(summary.get('run', {}).get('failure_reason') if isinstance(summary.get('run'), dict) else None)}`",
        f"- run return code: `{_markdown_inline(summary.get('run', {}).get('return_code') if isinstance(summary.get('run'), dict) else None)}`",
        f"- pipeline status: `{_markdown_inline(summary.get('pipeline', {}).get('status') if isinstance(summary.get('pipeline'), dict) else None)}`",
        f"- output inspection: `{_markdown_inline(summary.get('output_inspection', {}).get('status') if isinstance(summary.get('output_inspection'), dict) else None)}`",
        f"- runner smoke: `{_markdown_inline(summary.get('runner_smoke', {}).get('status') if isinstance(summary.get('runner_smoke'), dict) else None)}`",
        f"- output comparison: `{_markdown_inline(summary.get('output_comparison', {}).get('status') if isinstance(summary.get('output_comparison'), dict) else None)}`",
        f"- summary json: `{summary_path}`",
        "",
    ]

    output_comparison = summary.get("output_comparison")
    if isinstance(output_comparison, dict):
        mismatch_reasons = output_comparison.get("mismatch_reasons", [])
        lines.extend(
            [
                "## Mismatch Reasons",
                f"- reasons: `{_markdown_list(mismatch_reasons)}`",
                f"- unexpected output count: `{_markdown_inline(output_comparison.get('unexpected_output_count'))}`",
                "",
            ]
        )

    forced_options = summary.get("forced_options")
    if isinstance(forced_options, dict) and forced_options:
        forced_rows = [[key, forced_options[key]] for key in sorted(forced_options)]
        lines.extend(
            [
                "## Forced Options",
                _markdown_table(["Option", "Value"], forced_rows),
                "",
            ]
        )

    comparison_table = summary.get("comparison_table")
    if isinstance(comparison_table, dict):
        sensor_rows_raw = comparison_table.get("sensor_rows", [])
        if isinstance(sensor_rows_raw, list) and sensor_rows_raw:
            sensor_rows = [
                [
                    row.get("sensor_id"),
                    row.get("sensor_name"),
                    row.get("status"),
                    _markdown_list(row.get("found_output_roles", [])),
                    _markdown_list(row.get("missing_output_roles", [])),
                    _markdown_list(row.get("mismatch_reasons", [])),
                ]
                for row in sensor_rows_raw
                if isinstance(row, dict)
            ]
            lines.extend(
                [
                    "## Sensor Status",
                    _markdown_table(
                        [
                            "Sensor ID",
                            "Sensor Name",
                            "Status",
                            "Found Roles",
                            "Missing Roles",
                            "Mismatch Reasons",
                        ],
                        sensor_rows,
                    ),
                    "",
                ]
            )
        role_rows_raw = comparison_table.get("role_rows", [])
        if isinstance(role_rows_raw, list) and role_rows_raw:
            role_rows = [
                [
                    row.get("sensor_id"),
                    row.get("output_role"),
                    row.get("status"),
                    row.get("found_output_count"),
                    row.get("missing_output_count"),
                    _markdown_list(row.get("mismatch_reasons", [])),
                    _markdown_list(row.get("expected_backend_filenames", [])),
                    _markdown_list(row.get("discovered_backend_filenames", [])),
                ]
                for row in role_rows_raw
                if isinstance(row, dict)
            ]
            lines.extend(
                [
                    "## Role Status",
                    _markdown_table(
                        [
                            "Sensor ID",
                            "Output Role",
                            "Status",
                            "Found Count",
                            "Missing Count",
                            "Mismatch Reasons",
                            "Expected Files",
                            "Discovered Files",
                        ],
                        role_rows,
                    ),
                    "",
                ]
            )

    artifacts = summary.get("artifacts")
    if isinstance(artifacts, dict) and artifacts:
        artifact_rows = [[key, value] for key, value in sorted(artifacts.items())]
        lines.extend(
            [
                "## Artifacts",
                _markdown_table(["Artifact", "Path"], artifact_rows),
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _html_cell(value: Any) -> str:
    if isinstance(value, list):
        rendered = ", ".join(_markdown_inline(item) for item in value if _markdown_inline(item) != "-")
        return html.escape(rendered or "-")
    return html.escape(_markdown_inline(value))


def _html_table(headers: list[str], rows: list[list[Any]]) -> str:
    header_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    row_html = []
    for row in rows:
        row_html.append("<tr>" + "".join(f"<td>{_html_cell(cell)}</td>" for cell in row) + "</tr>")
    body = "".join(row_html) if row_html else "<tr><td colspan=\"{}\">-</td></tr>".format(len(headers))
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{body}</tbody></table>"


def _render_html_report(summary: dict[str, Any], summary_path: Path) -> str:
    overview_rows = [
        ["backend", summary.get("backend")],
        ["success", summary.get("success")],
        ["run status", summary.get("run", {}).get("status") if isinstance(summary.get("run"), dict) else None],
        [
            "failure reason",
            summary.get("run", {}).get("failure_reason")
            if isinstance(summary.get("run"), dict)
            else None,
        ],
        [
            "run return code",
            summary.get("run", {}).get("return_code")
            if isinstance(summary.get("run"), dict)
            else None,
        ],
        ["pipeline status", summary.get("pipeline", {}).get("status") if isinstance(summary.get("pipeline"), dict) else None],
        [
            "output inspection",
            summary.get("output_inspection", {}).get("status")
            if isinstance(summary.get("output_inspection"), dict)
            else None,
        ],
        [
            "runner smoke",
            summary.get("runner_smoke", {}).get("status")
            if isinstance(summary.get("runner_smoke"), dict)
            else None,
        ],
        [
            "output comparison",
            summary.get("output_comparison", {}).get("status")
            if isinstance(summary.get("output_comparison"), dict)
            else None,
        ],
        ["summary json", str(summary_path)],
    ]
    sections = [
        "<h1>Renderer Backend Smoke Report</h1>",
        "<h2>Overview</h2>",
        _html_table(["Field", "Value"], overview_rows),
    ]

    output_comparison = summary.get("output_comparison")
    if isinstance(output_comparison, dict):
        sections.extend(
            [
                "<h2>Mismatch Reasons</h2>",
                _html_table(
                    ["Field", "Value"],
                    [
                        ["reasons", output_comparison.get("mismatch_reasons", [])],
                        ["unexpected output count", output_comparison.get("unexpected_output_count")],
                    ],
                ),
            ]
        )

    forced_options = summary.get("forced_options")
    if isinstance(forced_options, dict) and forced_options:
        sections.extend(
            [
                "<h2>Forced Options</h2>",
                _html_table(
                    ["Option", "Value"],
                    [[key, forced_options[key]] for key in sorted(forced_options)],
                ),
            ]
        )

    comparison_table = summary.get("comparison_table")
    if isinstance(comparison_table, dict):
        sensor_rows_raw = comparison_table.get("sensor_rows", [])
        if isinstance(sensor_rows_raw, list) and sensor_rows_raw:
            sections.extend(
                [
                    "<h2>Sensor Status</h2>",
                    _html_table(
                        [
                            "Sensor ID",
                            "Sensor Name",
                            "Status",
                            "Found Roles",
                            "Missing Roles",
                            "Mismatch Reasons",
                        ],
                        [
                            [
                                row.get("sensor_id"),
                                row.get("sensor_name"),
                                row.get("status"),
                                row.get("found_output_roles", []),
                                row.get("missing_output_roles", []),
                                row.get("mismatch_reasons", []),
                            ]
                            for row in sensor_rows_raw
                            if isinstance(row, dict)
                        ],
                    ),
                ]
            )
        role_rows_raw = comparison_table.get("role_rows", [])
        if isinstance(role_rows_raw, list) and role_rows_raw:
            sections.extend(
                [
                    "<h2>Role Status</h2>",
                    _html_table(
                        [
                            "Sensor ID",
                            "Output Role",
                            "Status",
                            "Found Count",
                            "Missing Count",
                            "Mismatch Reasons",
                            "Expected Files",
                            "Discovered Files",
                        ],
                        [
                            [
                                row.get("sensor_id"),
                                row.get("output_role"),
                                row.get("status"),
                                row.get("found_output_count"),
                                row.get("missing_output_count"),
                                row.get("mismatch_reasons", []),
                                row.get("expected_backend_filenames", []),
                                row.get("discovered_backend_filenames", []),
                            ]
                            for row in role_rows_raw
                            if isinstance(row, dict)
                        ],
                    ),
                ]
            )

    artifacts = summary.get("artifacts")
    if isinstance(artifacts, dict) and artifacts:
        sections.extend(
            [
                "<h2>Artifacts</h2>",
                _html_table(
                    ["Artifact", "Path"],
                    [[key, value] for key, value in sorted(artifacts.items())],
                ),
            ]
        )

    body = "\n".join(sections)
    return (
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <title>Renderer Backend Smoke Report</title>\n"
        "  <style>\n"
        "    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #111827; }\n"
        "    h1, h2 { margin-bottom: 12px; }\n"
        "    table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }\n"
        "    th, td { border: 1px solid #d1d5db; padding: 8px; text-align: left; vertical-align: top; }\n"
        "    th { background: #f3f4f6; }\n"
        "    code { background: #f3f4f6; padding: 2px 4px; border-radius: 4px; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        f"{body}\n"
        "</body>\n"
        "</html>\n"
    )


def _build_effective_config(
    *,
    base_config: dict[str, Any],
    backend: str,
    output_dir_override: Path | None,
    backend_bin_override: str | None,
    renderer_map_override: str | None,
    option_overrides: list[str],
    repo_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    effective = dict(_resolve_env_payload(base_config))
    options = dict(effective.get("options", {}))
    forced_options: dict[str, Any] = {
        "renderer_bridge_enabled": True,
        "renderer_backend": backend,
        "renderer_execute": True,
        "renderer_execute_via_runner": True,
        "renderer_execute_and_inspect_via_runner": True,
        "renderer_fail_on_error": True,
        "renderer_cwd": str(repo_root),
        f"{backend}_wrapper": str((repo_root / "scripts" / f"renderer_launch_{backend}.sh").resolve()),
    }
    if backend_bin_override:
        forced_options[f"{backend}_bin"] = str(_resolve_runtime_path(backend_bin_override))
    if renderer_map_override is not None:
        forced_options["renderer_map"] = renderer_map_override

    for key, value in forced_options.items():
        options[key] = value
    for raw_override in option_overrides:
        dotted_key, value = _parse_option_override(raw_override)
        _set_dotted_option(options, dotted_key, value)

    effective["options"] = options
    effective["scenario_path"] = str(_resolve_runtime_path(effective["scenario_path"]))
    if output_dir_override is not None:
        effective["output_dir"] = str(_resolve_runtime_path(output_dir_override))
    elif "output_dir" in effective:
        effective["output_dir"] = str(_resolve_runtime_path(effective["output_dir"]))
    if effective.get("helios_bin"):
        effective["helios_bin"] = str(_resolve_runtime_path(effective["helios_bin"]))
    for option_key in (
        "renderer_cwd",
        "renderer_bin",
        "awsim_bin",
        "carla_bin",
        "awsim_wrapper",
        "carla_wrapper",
    ):
        option_value = options.get(option_key)
        if option_value:
            options[option_key] = str(_resolve_runtime_path(option_value))
    return effective, forced_options


def build_renderer_backend_smoke_summary(
    *,
    result: SensorSimResult,
    config_path: Path,
    effective_config_path: Path,
    backend: str,
    forced_options: dict[str, Any],
) -> dict[str, Any]:
    artifact_paths = {key: str(path) for key, path in result.artifacts.items()}
    backend_run_manifest = _load_artifact_payload(result.artifacts.get("backend_run_manifest"))
    pipeline_summary = _load_artifact_payload(result.artifacts.get("renderer_pipeline_summary"))
    output_inspection_manifest = _load_artifact_payload(
        result.artifacts.get("backend_output_inspection_manifest")
    )
    runner_smoke_manifest = _load_artifact_payload(
        result.artifacts.get("backend_runner_smoke_manifest")
    )
    output_smoke_report = _load_artifact_payload(result.artifacts.get("backend_output_smoke_report"))
    output_comparison_report = _load_artifact_payload(
        result.artifacts.get("backend_output_comparison_report")
    )
    comparison_table = _build_comparison_table(output_comparison_report)

    summary = {
        "config_path": str(config_path),
        "effective_config_path": str(effective_config_path),
        "backend": backend,
        "result_backend": result.backend,
        "success": result.success,
        "message": result.message,
        "forced_options": forced_options,
        "artifacts": artifact_paths,
        "metrics": result.metrics,
        "run": {
            "status": backend_run_manifest.get("status") if isinstance(backend_run_manifest, dict) else None,
            "failure_reason": (
                backend_run_manifest.get("failure_reason")
                if isinstance(backend_run_manifest, dict)
                else None
            ),
            "return_code": (
                backend_run_manifest.get("return_code")
                if isinstance(backend_run_manifest, dict)
                else None
            ),
        },
        "pipeline": {
            "status": pipeline_summary.get("status") if isinstance(pipeline_summary, dict) else None,
            "success": pipeline_summary.get("success") if isinstance(pipeline_summary, dict) else None,
        },
        "output_inspection": (
            {
                "status": output_inspection_manifest.get("status"),
                "success": output_inspection_manifest.get("success"),
                "return_code": output_inspection_manifest.get("return_code"),
                "message": output_inspection_manifest.get("message"),
            }
            if isinstance(output_inspection_manifest, dict)
            else None
        ),
        "runner_smoke": (
            {
                "status": runner_smoke_manifest.get("status"),
                "success": runner_smoke_manifest.get("success"),
                "return_code": runner_smoke_manifest.get("return_code"),
                "message": runner_smoke_manifest.get("message"),
            }
            if isinstance(runner_smoke_manifest, dict)
            else None
        ),
        "output_smoke_report": (
            {
                "status": output_smoke_report.get("status"),
                "coverage_ratio": output_smoke_report.get("coverage_ratio"),
            }
            if isinstance(output_smoke_report, dict)
            else None
        ),
        "output_comparison": (
            {
                "status": output_comparison_report.get("status"),
                "mismatch_reasons": output_comparison_report.get("mismatch_reasons", []),
                "unexpected_output_count": output_comparison_report.get(
                    "unexpected_output_count"
                ),
            }
            if isinstance(output_comparison_report, dict)
            else None
        ),
        "comparison_table": comparison_table,
    }
    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    config_path = args.config.expanduser().resolve()
    base_config = _load_json(config_path)
    effective_config, forced_options = _build_effective_config(
        base_config=base_config,
        backend=args.backend,
        output_dir_override=args.output_dir,
        backend_bin_override=args.backend_bin,
        renderer_map_override=args.renderer_map,
        option_overrides=list(args.set_option),
        repo_root=repo_root,
    )
    output_dir = _resolve_runtime_path(effective_config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    effective_config_path = output_dir / "renderer_backend_smoke_config.json"
    _write_json(effective_config_path, effective_config)

    mode = BackendMode(effective_config.get("mode", BackendMode.HYBRID_AUTO.value))
    options = dict(effective_config.get("options", {}))
    if effective_config.get("helios_runtime") and "helios_runtime" not in options:
        options["helios_runtime"] = effective_config["helios_runtime"]
    request = SensorSimRequest(
        scenario_path=Path(effective_config["scenario_path"]),
        output_dir=output_dir,
        sensor_profile=effective_config.get("sensor_profile", "default"),
        seed=int(effective_config.get("seed", 0)),
        options=options,
    )
    orchestrator = HybridOrchestrator(
        helios=HeliosAdapter(
            helios_bin=(
                Path(effective_config["helios_bin"])
                if effective_config.get("helios_bin")
                else None
            )
        ),
        native=NativePhysicsBackend(),
    )
    result = orchestrator.run(request, mode)
    summary = build_renderer_backend_smoke_summary(
        result=result,
        config_path=config_path,
        effective_config_path=effective_config_path,
        backend=args.backend,
        forced_options=forced_options,
    )
    summary_path = (
        _resolve_runtime_path(args.summary_path)
        if args.summary_path is not None
        else output_dir / "renderer_backend_smoke_summary.json"
    )
    markdown_report_path = (
        _resolve_runtime_path(args.markdown_report_path)
        if args.markdown_report_path is not None
        else output_dir / "renderer_backend_smoke_report.md"
    )
    html_report_path = (
        _resolve_runtime_path(args.html_report_path)
        if args.html_report_path is not None
        else output_dir / "renderer_backend_smoke_report.html"
    )
    summary["reports"] = {
        "markdown": str(markdown_report_path),
        "html": str(html_report_path),
    }
    _write_json(summary_path, summary)
    _write_text(markdown_report_path, _render_markdown_report(summary, summary_path))
    _write_text(html_report_path, _render_html_report(summary, summary_path))
    print(json.dumps(summary, indent=2))
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
