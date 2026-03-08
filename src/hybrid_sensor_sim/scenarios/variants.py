from __future__ import annotations

import itertools
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGICAL_SCENARIOS_SCHEMA_VERSION_V0 = "logical_scenarios_v0"
SCENARIO_VARIANTS_REPORT_SCHEMA_VERSION_V0 = "scenario_variants_report_v0"


def _must_be_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _must_be_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return value


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _build_combinations(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    if not parameters:
        return [{}]
    names: list[str] = []
    values_by_name: list[list[Any]] = []
    for key, value in parameters.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("parameter names must be non-empty strings")
        choices = _must_be_list(value, f"parameter '{key}'")
        if not choices:
            raise ValueError(f"parameter '{key}' choices must not be empty")
        names.append(key)
        values_by_name.append(choices)

    rows: list[dict[str, Any]] = []
    for combo in itertools.product(*values_by_name):
        rows.append({name: combo[idx] for idx, name in enumerate(names)})
    return rows


def _sample_rows(
    rows: list[dict[str, Any]],
    *,
    sampling: str,
    sample_size: int,
    max_variants_per_scenario: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    capped = rows[:max_variants_per_scenario]
    if sampling == "full":
        return capped
    if sample_size <= 0:
        raise ValueError("sample_size must be positive when sampling=random")
    target = min(sample_size, len(capped))
    indices = sorted(rng.sample(range(len(capped)), target))
    return [capped[idx] for idx in indices]


def validate_logical_scenarios_payload(payload: dict[str, Any]) -> dict[str, Any]:
    schema_version = str(payload.get("logical_scenarios_schema_version", "")).strip()
    if schema_version and schema_version != LOGICAL_SCENARIOS_SCHEMA_VERSION_V0:
        raise ValueError(
            "logical_scenarios_schema_version must be "
            f"{LOGICAL_SCENARIOS_SCHEMA_VERSION_V0}"
        )
    logical_scenarios = _must_be_list(payload.get("logical_scenarios"), "logical_scenarios")
    normalized_rows: list[dict[str, Any]] = []
    for idx, logical in enumerate(logical_scenarios):
        logical_obj = _must_be_dict(logical, f"logical_scenarios[{idx}]")
        logical_id = str(logical_obj.get("scenario_id", "")).strip()
        if not logical_id:
            raise ValueError("logical scenario entry missing scenario_id")
        parameters = _must_be_dict(logical_obj.get("parameters"), "logical scenario parameters")
        normalized_rows.append(
            {
                "scenario_id": logical_id,
                "parameters": parameters,
            }
        )
    return {
        "logical_scenarios_schema_version": LOGICAL_SCENARIOS_SCHEMA_VERSION_V0,
        "logical_scenarios": normalized_rows,
    }


def load_logical_scenarios_source(
    *,
    logical_scenarios_path: str = "",
    scenario_language_profile: str = "",
    scenario_language_dir: str | Path | None = None,
) -> tuple[dict[str, Any], Path, str]:
    logical_scenarios_text = str(logical_scenarios_path).strip()
    scenario_language_profile_text = str(scenario_language_profile).strip()
    if bool(logical_scenarios_text) == bool(scenario_language_profile_text):
        raise ValueError("provide exactly one of logical_scenarios_path or scenario_language_profile")

    if logical_scenarios_text:
        source_path = Path(logical_scenarios_text).resolve()
        payload = _load_json_object(source_path, "logical scenario file")
        return validate_logical_scenarios_payload(payload), source_path, "logical_scenarios"

    if scenario_language_dir is None:
        raise ValueError("scenario_language_dir is required when scenario_language_profile is used")
    scenario_language_root = Path(scenario_language_dir).resolve()
    profile_path = scenario_language_root / f"{scenario_language_profile_text}.json"
    payload = _load_json_object(profile_path, "scenario language profile")
    profile_id = str(payload.get("profile_id", "")).strip()
    if profile_id and profile_id != scenario_language_profile_text:
        raise ValueError(
            "scenario language profile_id mismatch: "
            f"expected={scenario_language_profile_text} actual={profile_id}"
        )
    payload = dict(payload)
    payload["logical_scenarios_schema_version"] = LOGICAL_SCENARIOS_SCHEMA_VERSION_V0
    return validate_logical_scenarios_payload(payload), profile_path, "scenario_language_profile"


def generate_variants(
    payload: dict[str, Any],
    *,
    sampling: str,
    sample_size: int,
    max_variants_per_scenario: int,
    seed: int,
) -> list[dict[str, Any]]:
    normalized = validate_logical_scenarios_payload(payload)
    rng = random.Random(seed)
    variants: list[dict[str, Any]] = []
    for logical in normalized["logical_scenarios"]:
        logical_id = str(logical["scenario_id"])
        parameters = dict(logical["parameters"])
        base_rows = _build_combinations(parameters)
        selected_rows = _sample_rows(
            base_rows,
            sampling=sampling,
            sample_size=sample_size,
            max_variants_per_scenario=max_variants_per_scenario,
            rng=rng,
        )
        for idx, row in enumerate(selected_rows, start=1):
            variants.append(
                {
                    "scenario_id": f"{logical_id}_{idx:04d}",
                    "logical_scenario_id": logical_id,
                    "parameters": row,
                }
            )
    return variants


def build_scenario_variants_report(
    *,
    payload: dict[str, Any],
    source_path: Path,
    source_kind: str,
    sampling: str,
    sample_size: int,
    max_variants_per_scenario: int,
    seed: int,
) -> dict[str, Any]:
    normalized = validate_logical_scenarios_payload(payload)
    variants = generate_variants(
        normalized,
        sampling=sampling,
        sample_size=sample_size,
        max_variants_per_scenario=max_variants_per_scenario,
        seed=seed,
    )
    return {
        "scenario_variants_report_schema_version": SCENARIO_VARIANTS_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_path": str(source_path),
        "source_kind": source_kind,
        "logical_scenarios_schema_version": LOGICAL_SCENARIOS_SCHEMA_VERSION_V0,
        "sampling": sampling,
        "sample_size": int(sample_size),
        "seed": int(seed),
        "max_variants_per_scenario": int(max_variants_per_scenario),
        "scenario_count": len(normalized["logical_scenarios"]),
        "variant_count": len(variants),
        "variants": variants,
    }
