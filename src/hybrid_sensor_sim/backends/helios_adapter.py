from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult


class HeliosAdapter(SensorBackend):
    def __init__(self, helios_bin: Path | None = None) -> None:
        self._helios_bin = helios_bin

    def name(self) -> str:
        return "helios"

    def _resolve_binary(self) -> Path | None:
        if self._helios_bin is not None:
            return self._helios_bin
        env_bin = os.getenv("HELIOS_BIN")
        return Path(env_bin) if env_bin else None

    def _available(self, binary: Path | None) -> bool:
        return bool(binary and binary.exists() and os.access(binary, os.X_OK))

    def simulate(self, request: SensorSimRequest) -> SensorSimResult:
        binary = self._resolve_binary()
        helios_output = request.output_dir / "helios_raw"
        helios_output.mkdir(parents=True, exist_ok=True)

        planned = {
            "binary": str(binary) if binary else None,
            "scenario": str(request.scenario_path),
            "sensor_profile": request.sensor_profile,
            "seed": request.seed,
        }
        manifest_path = helios_output / "helios_execution_plan.json"
        manifest_path.write_text(json.dumps(planned, indent=2), encoding="utf-8")

        if not self._available(binary):
            return SensorSimResult(
                backend=self.name(),
                success=False,
                artifacts={"execution_plan": manifest_path},
                message="HELIOS binary is missing or not executable.",
            )

        execute = bool(request.options.get("execute_helios", False))
        if execute:
            command = request.options.get("helios_command", [str(binary)])
            process = subprocess.run(  # noqa: S603
                command,
                cwd=str(helios_output),
                text=True,
                capture_output=True,
                check=False,
            )
            (helios_output / "stdout.log").write_text(process.stdout, encoding="utf-8")
            (helios_output / "stderr.log").write_text(process.stderr, encoding="utf-8")
            if process.returncode != 0:
                return SensorSimResult(
                    backend=self.name(),
                    success=False,
                    artifacts={
                        "execution_plan": manifest_path,
                        "stdout": helios_output / "stdout.log",
                        "stderr": helios_output / "stderr.log",
                    },
                    message=f"HELIOS command failed with exit code {process.returncode}.",
                )

        return SensorSimResult(
            backend=self.name(),
            success=True,
            artifacts={"execution_plan": manifest_path},
            message="HELIOS adapter completed.",
        )

