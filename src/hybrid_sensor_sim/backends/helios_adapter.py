from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Iterable

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult


class HeliosAdapter(SensorBackend):
    _OUTPUT_DIR_RE = re.compile(r'Output directory:\s*"([^"]+)"')

    def __init__(self, helios_bin: Path | None = None) -> None:
        self._helios_bin = helios_bin

    def name(self) -> str:
        return "helios"

    def _resolve_binary(self) -> Path | None:
        if self._helios_bin is not None:
            return self._helios_bin
        env_bin = os.getenv("HELIOS_BIN")
        if env_bin:
            return Path(env_bin)
        for candidate in self._default_binary_candidates():
            if candidate.exists() and os.access(candidate, os.X_OK):
                return candidate
        return None

    def _available(self, binary: Path | None) -> bool:
        return bool(binary and binary.exists() and os.access(binary, os.X_OK))

    def _default_binary_candidates(self) -> list[Path]:
        cwd = Path.cwd()
        names = [
            "third_party/helios/build/helios++",
            "third_party/helios/build/Release/helios++",
            "third_party/helios/build/Debug/helios++",
            "third_party/helios/build/pyhelios/bin/helios++",
            "third_party/helios/helios++",
        ]
        return [(cwd / item).resolve() for item in names]

    def _resolve_helios_cwd(self, request: SensorSimRequest, binary: Path) -> Path:
        configured = request.options.get("helios_cwd")
        if configured:
            path = Path(str(configured)).expanduser()
            return path if path.is_absolute() else (Path.cwd() / path).resolve()

        binary_parent = binary.resolve().parent
        if binary_parent.name in {"Debug", "Release"} and binary_parent.parent.name == "build":
            return binary_parent.parent.parent
        if binary_parent.name == "build":
            return binary_parent.parent
        return Path.cwd()

    def _resolve_input_path(self, raw: str | Path, helios_cwd: Path) -> Path:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else (helios_cwd / path).resolve()

    def _resolve_output_root(self, request: SensorSimRequest) -> Path:
        configured = request.options.get("helios_output_root")
        if configured:
            path = Path(str(configured)).expanduser()
            return path if path.is_absolute() else (request.output_dir / path).resolve()
        return (request.output_dir / "helios_output").resolve()

    def _build_helios_command(
        self,
        binary: Path,
        request: SensorSimRequest,
        helios_cwd: Path,
        output_root: Path,
    ) -> tuple[list[str], Path, list[Path]]:
        options = request.options
        survey_raw = options.get("survey_path", request.scenario_path)
        survey_path = self._resolve_input_path(survey_raw, helios_cwd)

        assets_paths_raw = options.get("assets_paths", [])
        assets_paths = [self._resolve_input_path(item, helios_cwd) for item in assets_paths_raw]

        command = [str(binary), str(survey_path), "--output", str(output_root)]
        for assets_path in assets_paths:
            command.extend(["--assets", str(assets_path)])
        command.extend(["--seed", str(request.seed)])

        bool_flags = {
            "write_waveform": "--writeWaveform",
            "write_pulse": "--writePulse",
            "calc_echowidth": "--calcEchowidth",
            "fullwave_noise": "--fullwaveNoise",
            "split_by_channel": "--splitByChannel",
            "disable_platform_noise": "--disablePlatformNoise",
            "disable_leg_noise": "--disableLegNoise",
            "rebuild_scene": "--rebuildScene",
            "no_scene_writing": "--noSceneWriting",
            "las_output": "--lasOutput",
            "las10": "--las10",
            "zip_output": "--zipOutput",
            "fixed_incidence_angle": "--fixedIncidenceAngle",
        }
        for key, flag in bool_flags.items():
            if bool(options.get(key, False)):
                command.append(flag)

        scalar_flags = {
            "gps_start_time": "--gpsStartTime",
            "las_scale": "--lasScale",
            "parallelization": "--parallelization",
            "nthreads": "--nthreads",
            "chunk_size": "--chunkSize",
            "warehouse_factor": "--warehouseFactor",
            "kdt": "--kdt",
            "kdt_jobs": "--kdtJobs",
            "kdt_geom_jobs": "--kdtGeomJobs",
            "sah_nodes": "--sahNodes",
        }
        for key, flag in scalar_flags.items():
            if key in options:
                command.extend([flag, str(options[key])])

        command.extend(str(item) for item in options.get("extra_args", []))
        return command, survey_path, assets_paths

    def _extract_output_dir_from_logs(self, stdout: str, stderr: str) -> Path | None:
        combined = "\n".join([stdout, stderr])
        match = self._OUTPUT_DIR_RE.search(combined)
        if not match:
            return None
        return Path(match.group(1)).expanduser()

    def _collect_files(self, root: Path, exts: Iterable[str]) -> list[Path]:
        ext_set = {item.lower() for item in exts}
        return sorted(
            [path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in ext_set]
        )

    def _collect_string_match_files(self, root: Path, token: str) -> list[Path]:
        return sorted([path for path in root.rglob("*") if path.is_file() and token in path.name])

    def _build_output_summary(
        self,
        root_dir: Path,
        output_dir_manifest: Path,
    ) -> tuple[dict[str, Path], dict[str, float], dict[str, object]]:
        point_cloud_files = self._collect_files(root_dir, [".las", ".laz", ".xyz"])
        trajectory_files = self._collect_string_match_files(root_dir, "_trajectory")
        pulse_files = self._collect_string_match_files(root_dir, "_pulse")
        fullwave_files = self._collect_string_match_files(root_dir, "_fullwave")

        artifacts: dict[str, Path] = {"helios_output_root": root_dir, "output_manifest": output_dir_manifest}
        if point_cloud_files:
            artifacts["point_cloud_primary"] = point_cloud_files[0]
        if trajectory_files:
            artifacts["trajectory_primary"] = trajectory_files[0]
        if pulse_files:
            artifacts["pulse_primary"] = pulse_files[0]
        if fullwave_files:
            artifacts["fullwave_primary"] = fullwave_files[0]

        metrics = {
            "point_cloud_file_count": float(len(point_cloud_files)),
            "trajectory_file_count": float(len(trajectory_files)),
            "pulse_file_count": float(len(pulse_files)),
            "fullwave_file_count": float(len(fullwave_files)),
        }
        manifest = {
            "root_dir": str(root_dir),
            "point_cloud_files": [str(path) for path in point_cloud_files],
            "trajectory_files": [str(path) for path in trajectory_files],
            "pulse_files": [str(path) for path in pulse_files],
            "fullwave_files": [str(path) for path in fullwave_files],
            "metrics": metrics,
        }
        return artifacts, metrics, manifest

    def simulate(self, request: SensorSimRequest) -> SensorSimResult:
        binary = self._resolve_binary()
        helios_output = request.output_dir / "helios_raw"
        helios_output.mkdir(parents=True, exist_ok=True)

        if not self._available(binary):
            planned = {
                "binary": str(binary) if binary else None,
                "scenario": str(request.scenario_path),
                "sensor_profile": request.sensor_profile,
                "seed": request.seed,
                "error": "HELIOS binary is missing or not executable.",
            }
            manifest_path = helios_output / "helios_execution_plan.json"
            manifest_path.write_text(json.dumps(planned, indent=2), encoding="utf-8")
            return SensorSimResult(
                backend=self.name(),
                success=False,
                artifacts={"execution_plan": manifest_path},
                message="HELIOS binary is missing or not executable.",
            )

        assert binary is not None
        helios_cwd = self._resolve_helios_cwd(request, binary)
        output_root = self._resolve_output_root(request)
        command_override = request.options.get("helios_command")
        if command_override:
            command = [str(item) for item in command_override]
            survey_path = request.scenario_path
            assets_paths = []
        else:
            command, survey_path, assets_paths = self._build_helios_command(
                binary=binary,
                request=request,
                helios_cwd=helios_cwd,
                output_root=output_root,
            )

        planned = {
            "binary": str(binary),
            "survey_path": str(survey_path),
            "assets_paths": [str(path) for path in assets_paths],
            "sensor_profile": request.sensor_profile,
            "seed": request.seed,
            "helios_cwd": str(helios_cwd),
            "output_root": str(output_root),
            "command": command,
            "used_command_override": bool(command_override),
        }
        manifest_path = helios_output / "helios_execution_plan.json"
        manifest_path.write_text(json.dumps(planned, indent=2), encoding="utf-8")

        execute = bool(request.options.get("execute_helios", False))
        if not execute:
            return SensorSimResult(
                backend=self.name(),
                success=True,
                artifacts={"execution_plan": manifest_path},
                message="HELIOS execution plan generated only (execute_helios=false).",
            )

        process = subprocess.run(  # noqa: S603
            command,
            cwd=str(helios_cwd),
            text=True,
            capture_output=True,
            check=False,
        )
        stdout_path = helios_output / "stdout.log"
        stderr_path = helios_output / "stderr.log"
        stdout_path.write_text(process.stdout, encoding="utf-8")
        stderr_path.write_text(process.stderr, encoding="utf-8")
        if process.returncode != 0:
            return SensorSimResult(
                backend=self.name(),
                success=False,
                artifacts={
                    "execution_plan": manifest_path,
                    "stdout": stdout_path,
                    "stderr": stderr_path,
                },
                message=f"HELIOS command failed with exit code {process.returncode}.",
            )

        detected_output = self._extract_output_dir_from_logs(process.stdout, process.stderr)
        if detected_output is None:
            detected_output = output_root
        if not detected_output.is_absolute():
            detected_output = (helios_cwd / detected_output).resolve()
        output_root.mkdir(parents=True, exist_ok=True)

        output_manifest_path = helios_output / "helios_output_manifest.json"
        artifacts, metrics, output_manifest = self._build_output_summary(
            root_dir=detected_output,
            output_dir_manifest=output_manifest_path,
        )
        output_manifest["detected_output_dir"] = str(detected_output)
        output_manifest["return_code"] = process.returncode
        output_manifest_path.write_text(json.dumps(output_manifest, indent=2), encoding="utf-8")
        artifacts["execution_plan"] = manifest_path
        artifacts["stdout"] = stdout_path
        artifacts["stderr"] = stderr_path

        return SensorSimResult(
            backend=self.name(),
            success=True,
            artifacts=artifacts,
            metrics=metrics,
            message="HELIOS adapter completed.",
        )
