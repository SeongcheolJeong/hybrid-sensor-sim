from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hybrid HELIOS sensor simulation runner")
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to JSON config file",
    )
    return parser.parse_args()


def _load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> int:
    args = _parse_args()
    cfg = _load_config(args.config)

    mode = BackendMode(cfg.get("mode", BackendMode.HYBRID_AUTO.value))
    options = dict(cfg.get("options", {}))
    if cfg.get("helios_runtime") and "helios_runtime" not in options:
        options["helios_runtime"] = cfg["helios_runtime"]
    request = SensorSimRequest(
        scenario_path=Path(cfg["scenario_path"]),
        output_dir=Path(cfg["output_dir"]),
        sensor_profile=cfg.get("sensor_profile", "default"),
        seed=int(cfg.get("seed", 0)),
        options=options,
    )
    request.output_dir.mkdir(parents=True, exist_ok=True)

    orchestrator = HybridOrchestrator(
        helios=HeliosAdapter(
            helios_bin=Path(cfg["helios_bin"]) if cfg.get("helios_bin") else None
        ),
        native=NativePhysicsBackend(),
    )
    result = orchestrator.run(request, mode)

    response = {
        "backend": result.backend,
        "success": result.success,
        "message": result.message,
        "artifacts": {k: str(v) for k, v in result.artifacts.items()},
        "metrics": result.metrics,
    }
    print(json.dumps(response, indent=2))
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
