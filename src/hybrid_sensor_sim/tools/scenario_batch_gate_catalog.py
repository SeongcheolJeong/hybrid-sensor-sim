from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SCENARIO_BATCH_GATE_PROFILE_SCHEMA_VERSION_V0 = "scenario_batch_gate_profile_v0"


def _load_json_dict(path: Path, *, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def default_scenario_batch_gate_profile_dir() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "autonomy_e2e"
        / "p_validation"
    )


def load_scenario_batch_gate_profile(path: Path) -> dict[str, Any]:
    payload = _load_json_dict(path, label="scenario batch gate profile")
    schema_version = str(payload.get("gate_profile_schema_version", "")).strip()
    if schema_version != SCENARIO_BATCH_GATE_PROFILE_SCHEMA_VERSION_V0:
        raise ValueError(
            "gate_profile_schema_version must be "
            f"{SCENARIO_BATCH_GATE_PROFILE_SCHEMA_VERSION_V0}"
        )
    policy = payload.get("policy")
    if not isinstance(policy, dict):
        raise ValueError("scenario batch gate profile missing policy block")
    profile_id = str(payload.get("profile_id", "")).strip()
    if not profile_id:
        raise ValueError("scenario batch gate profile missing profile_id")
    return payload


def build_scenario_batch_gate_profile_catalog(
    profile_dir: str | Path | None = None,
) -> dict[str, dict[str, Any]]:
    base_dir = Path(profile_dir).resolve() if profile_dir is not None else default_scenario_batch_gate_profile_dir()
    if not base_dir.is_dir():
        raise FileNotFoundError(f"scenario batch gate profile directory not found: {base_dir}")
    catalog: dict[str, dict[str, Any]] = {}
    for candidate in sorted(base_dir.glob("*.json")):
        try:
            payload = load_scenario_batch_gate_profile(candidate)
        except (ValueError, FileNotFoundError):
            continue
        profile_id = str(payload["profile_id"]).strip()
        catalog[profile_id] = {
            "profile_id": profile_id,
            "path": str(candidate.resolve()),
            "policy": dict(payload["policy"]),
        }
    return catalog


def resolve_scenario_batch_gate_profile_path(
    *,
    gate_profile: str,
    gate_profile_id: str,
    gate_profile_dir: str,
) -> Path | None:
    gate_profile_text = str(gate_profile).strip()
    gate_profile_id_text = str(gate_profile_id).strip()
    if gate_profile_text and gate_profile_id_text:
        raise ValueError("use either --gate-profile or --gate-profile-id, not both")
    if gate_profile_text:
        return Path(gate_profile_text).resolve()
    if not gate_profile_id_text:
        return None
    catalog = build_scenario_batch_gate_profile_catalog(
        Path(gate_profile_dir).resolve() if str(gate_profile_dir).strip() else None
    )
    if gate_profile_id_text not in catalog:
        available_ids = ", ".join(sorted(catalog)) or "<none>"
        raise ValueError(
            f"unknown scenario batch gate profile id: {gate_profile_id_text}; available: {available_ids}"
        )
    return Path(catalog[gate_profile_id_text]["path"]).resolve()
