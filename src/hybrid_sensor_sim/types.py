from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class BackendMode(str, Enum):
    HELIOS_ONLY = "helios_only"
    NATIVE_ONLY = "native_only"
    HYBRID_AUTO = "hybrid_auto"


@dataclass
class SensorSimRequest:
    scenario_path: Path
    output_dir: Path
    sensor_profile: str = "default"
    seed: int = 0
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class SensorSimResult:
    backend: str
    success: bool
    artifacts: dict[str, Path] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    message: str = ""
