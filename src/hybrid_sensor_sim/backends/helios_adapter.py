from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.io.survey_mapping import generate_survey_from_scenario
from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult


@dataclass(frozen=True)
class _PreparedExecution:
    command: list[str]
    cwd: Path
    runtime: str
    binary: str
    survey_path: Path
    assets_paths: list[Path]
    output_root: Path
    generated_survey_path: Path | None = None
    survey_mapping_metadata: dict[str, object] | None = None
    host_root: Path | None = None
    container_root: Path | None = None


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
            return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()

        binary_parent = binary.resolve().parent
        if binary_parent.name in {"Debug", "Release"} and binary_parent.parent.name == "build":
            return binary_parent.parent.parent
        if binary_parent.name == "build":
            return binary_parent.parent
        return Path.cwd()

    def _resolve_input_path(self, raw: str | Path, helios_cwd: Path) -> Path:
        path = Path(raw).expanduser()
        return path.resolve() if path.is_absolute() else (helios_cwd / path).resolve()

    def _resolve_output_root(self, request: SensorSimRequest) -> Path:
        configured = request.options.get("helios_output_root")
        if configured:
            path = Path(str(configured)).expanduser()
            return path.resolve() if path.is_absolute() else (request.output_dir / path).resolve()
        return (request.output_dir / "helios_output").resolve()

    def _resolve_generated_survey_output_dir(self, request: SensorSimRequest) -> Path:
        configured = request.options.get("survey_generated_output_dir")
        if configured:
            path = Path(str(configured)).expanduser()
            return path.resolve() if path.is_absolute() else (request.output_dir / path).resolve()
        return (request.output_dir / "helios_raw" / "generated_surveys").resolve()

    def _resolve_scenario_path_for_generation(
        self,
        request: SensorSimRequest,
        helios_cwd: Path,
    ) -> Path:
        raw = request.options.get("survey_scenario_path", request.scenario_path)
        return self._resolve_input_path(raw, helios_cwd)

    def _build_helios_args(
        self,
        request: SensorSimRequest,
        survey_path: Path,
        assets_paths: list[Path],
        output_root: Path,
    ) -> list[str]:
        options = request.options
        args = [str(survey_path), "--output", str(output_root), "--seed", str(request.seed)]
        for assets_path in assets_paths:
            args.extend(["--assets", str(assets_path)])

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
                args.append(flag)

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
                args.extend([flag, str(options[key])])

        args.extend(str(item) for item in options.get("extra_args", []))
        return args

    def _build_helios_command(
        self,
        binary: Path,
        request: SensorSimRequest,
        helios_cwd: Path,
        output_root: Path,
    ) -> tuple[list[str], Path, list[Path], Path | None, dict[str, object] | None]:
        options = request.options
        generated_survey_path: Path | None = None
        survey_mapping_metadata: dict[str, object] | None = None
        if bool(options.get("survey_generate_from_scenario", False)):
            scenario_path = self._resolve_scenario_path_for_generation(
                request=request,
                helios_cwd=helios_cwd,
            )
            survey_mapping_metadata = {}
            generated_survey_path = generate_survey_from_scenario(
                scenario_path=scenario_path,
                output_dir=self._resolve_generated_survey_output_dir(request),
                options=options,
                metadata_out=survey_mapping_metadata,
            )
            survey_path = generated_survey_path
        else:
            survey_raw = options.get("survey_path", request.scenario_path)
            survey_path = self._resolve_input_path(survey_raw, helios_cwd)

        assets_paths_raw = options.get("assets_paths", [])
        assets_paths = [self._resolve_input_path(item, helios_cwd) for item in assets_paths_raw]

        command = [str(binary), *self._build_helios_args(request, survey_path, assets_paths, output_root)]
        return command, survey_path, assets_paths, generated_survey_path, survey_mapping_metadata

    def _extract_output_dir_from_logs(self, stdout: str, stderr: str) -> Path | None:
        combined = "\n".join([stdout, stderr])
        match = self._OUTPUT_DIR_RE.search(combined)
        if not match:
            return None
        return Path(match.group(1)).expanduser()

    def _collect_files(self, root: Path, exts: Iterable[str]) -> list[Path]:
        ext_set = {item.lower() for item in exts}
        if not root.exists():
            return []
        return sorted(
            [path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in ext_set]
        )

    def _collect_string_match_files(self, root: Path, token: str) -> list[Path]:
        if not root.exists():
            return []
        return sorted([path for path in root.rglob("*") if path.is_file() and token in path.name])

    def _docker_daemon_available(self) -> tuple[bool, str]:
        try:
            proc = subprocess.run(
                ["docker", "info"],
                text=True,
                capture_output=True,
                check=False,
            )
        except FileNotFoundError:
            return False, "docker CLI is not installed."
        if proc.returncode != 0:
            stderr = proc.stderr.strip()
            return False, stderr if stderr else "docker daemon is not reachable."
        return True, ""

    def _ensure_path_under_root(self, path: Path, root: Path) -> bool:
        try:
            path.resolve().relative_to(root.resolve())
            return True
        except ValueError:
            return False

    def _host_to_container_path(self, path: Path, host_root: Path, container_root: Path) -> Path:
        relative = path.resolve().relative_to(host_root.resolve())
        return (container_root / relative).resolve()

    def _container_to_host_path(self, path: Path, host_root: Path, container_root: Path) -> Path:
        if path.is_absolute():
            try:
                relative = path.relative_to(container_root)
                return (host_root / relative).resolve()
            except ValueError:
                return path
        return (host_root / path).resolve()

    def _prepare_binary_execution(self, request: SensorSimRequest, binary: Path) -> _PreparedExecution:
        helios_cwd = self._resolve_helios_cwd(request, binary)
        output_root = self._resolve_output_root(request)
        command, survey_path, assets_paths, generated_survey_path, survey_mapping_metadata = (
            self._build_helios_command(
                binary=binary,
                request=request,
                helios_cwd=helios_cwd,
                output_root=output_root,
            )
        )
        return _PreparedExecution(
            command=command,
            cwd=helios_cwd,
            runtime="binary",
            binary=str(binary),
            survey_path=survey_path,
            assets_paths=assets_paths,
            output_root=output_root,
            generated_survey_path=generated_survey_path,
            survey_mapping_metadata=survey_mapping_metadata,
        )

    def _prepare_docker_execution(
        self,
        request: SensorSimRequest,
        require_runtime_available: bool,
    ) -> tuple[_PreparedExecution | None, str | None]:
        if require_runtime_available:
            docker_ok, docker_err = self._docker_daemon_available()
            if not docker_ok:
                return None, f"docker runtime requested but unavailable: {docker_err}"

        image = str(request.options.get("helios_docker_image", "")).strip()
        if not image:
            return None, "docker runtime requires 'helios_docker_image' option."

        host_root = Path.cwd().resolve()
        container_root = Path(str(request.options.get("helios_docker_mount_point", "/workspace")))
        container_binary = str(request.options.get("helios_docker_binary", "helios++"))

        configured_cwd = request.options.get("helios_cwd")
        if configured_cwd:
            helios_cwd = Path(str(configured_cwd)).expanduser()
            helios_cwd = helios_cwd.resolve() if helios_cwd.is_absolute() else (host_root / helios_cwd).resolve()
        else:
            helios_cwd = host_root

        generated_survey_path: Path | None = None
        survey_mapping_metadata: dict[str, object] | None = None
        if bool(request.options.get("survey_generate_from_scenario", False)):
            scenario_path = self._resolve_scenario_path_for_generation(
                request=request,
                helios_cwd=helios_cwd,
            )
            survey_mapping_metadata = {}
            generated_survey_path = generate_survey_from_scenario(
                scenario_path=scenario_path,
                output_dir=self._resolve_generated_survey_output_dir(request),
                options=request.options,
                metadata_out=survey_mapping_metadata,
            )
            survey_path = generated_survey_path
        else:
            survey_path = self._resolve_input_path(
                request.options.get("survey_path", request.scenario_path), helios_cwd
            )
        assets_paths_raw = request.options.get("assets_paths", [])
        assets_paths = [self._resolve_input_path(item, helios_cwd) for item in assets_paths_raw]
        output_root = self._resolve_output_root(request)

        required_paths = [helios_cwd, survey_path, output_root, *assets_paths]
        out_of_root = [path for path in required_paths if not self._ensure_path_under_root(path, host_root)]
        if out_of_root:
            return (
                None,
                "docker runtime supports paths under current workspace only. "
                f"out_of_root={','.join(str(path) for path in out_of_root)}",
            )

        container_cwd = self._host_to_container_path(helios_cwd, host_root, container_root)
        container_survey = self._host_to_container_path(survey_path, host_root, container_root)
        container_assets = [
            self._host_to_container_path(path, host_root, container_root) for path in assets_paths
        ]
        container_output = self._host_to_container_path(output_root, host_root, container_root)

        helios_args = self._build_helios_args(
            request=request,
            survey_path=container_survey,
            assets_paths=container_assets,
            output_root=container_output,
        )
        docker_extra_run_args = [str(item) for item in request.options.get("helios_docker_extra_run_args", [])]
        command = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{host_root}:{container_root}",
            "-w",
            str(container_cwd),
            *docker_extra_run_args,
            image,
            container_binary,
            *helios_args,
        ]
        return (
            _PreparedExecution(
                command=command,
                cwd=host_root,
                runtime="docker",
                binary=f"{image}:{container_binary}",
                survey_path=survey_path,
                assets_paths=assets_paths,
                output_root=output_root,
                generated_survey_path=generated_survey_path,
                survey_mapping_metadata=survey_mapping_metadata,
                host_root=host_root,
                container_root=container_root,
            ),
            None,
        )

    def _prepare_execution(
        self,
        request: SensorSimRequest,
        require_runtime_available: bool,
    ) -> tuple[_PreparedExecution | None, str | None]:
        runtime = str(request.options.get("helios_runtime", "auto")).lower().strip()
        if runtime not in {"auto", "binary", "docker"}:
            return None, f"unsupported helios_runtime '{runtime}'. expected one of auto/binary/docker."

        binary = self._resolve_binary()
        binary_available = self._available(binary)
        if runtime == "binary":
            if not binary_available or binary is None:
                return None, "binary runtime requested but HELIOS binary is missing or not executable."
            return self._prepare_binary_execution(request, binary), None

        if runtime == "docker":
            return self._prepare_docker_execution(request, require_runtime_available=require_runtime_available)

        if binary_available and binary is not None:
            return self._prepare_binary_execution(request, binary), None

        docker_exec, docker_err = self._prepare_docker_execution(
            request,
            require_runtime_available=require_runtime_available,
        )
        if docker_exec is not None:
            return docker_exec, None
        return (
            None,
            "auto runtime could not prepare execution: "
            "binary unavailable and docker unavailable. "
            f"details={docker_err}",
        )

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
        helios_output = request.output_dir / "helios_raw"
        helios_output.mkdir(parents=True, exist_ok=True)
        execute = bool(request.options.get("execute_helios", False))

        command_override = request.options.get("helios_command")
        prepared: _PreparedExecution | None = None
        preparation_error: str | None = None
        if command_override:
            cwd_raw = request.options.get("helios_cwd", Path.cwd())
            cwd = Path(str(cwd_raw)).expanduser()
            cwd = cwd.resolve() if cwd.is_absolute() else (Path.cwd() / cwd).resolve()
            prepared = _PreparedExecution(
                command=[str(item) for item in command_override],
                cwd=cwd,
                runtime="override",
                binary="command_override",
                survey_path=request.scenario_path.resolve(),
                assets_paths=[],
                output_root=self._resolve_output_root(request),
            )
        else:
            try:
                prepared, preparation_error = self._prepare_execution(
                    request,
                    require_runtime_available=execute,
                )
            except (ValueError, OSError) as exc:
                prepared = None
                preparation_error = str(exc)

        planned = {
            "binary": prepared.binary if prepared else None,
            "survey_path": str(prepared.survey_path) if prepared else str(request.scenario_path),
            "assets_paths": [str(path) for path in prepared.assets_paths] if prepared else [],
            "generated_survey_path": str(prepared.generated_survey_path)
            if prepared and prepared.generated_survey_path
            else None,
            "survey_generated_from_scenario": bool(prepared and prepared.generated_survey_path),
            "survey_mapping_metadata": prepared.survey_mapping_metadata if prepared else None,
            "sensor_profile": request.sensor_profile,
            "seed": request.seed,
            "runtime": prepared.runtime if prepared else str(request.options.get("helios_runtime", "auto")),
            "helios_cwd": str(prepared.cwd) if prepared else str(request.options.get("helios_cwd", "")),
            "output_root": str(prepared.output_root) if prepared else str(self._resolve_output_root(request)),
            "command": prepared.command if prepared else [],
            "used_command_override": bool(command_override),
            "error": preparation_error,
        }
        manifest_path = helios_output / "helios_execution_plan.json"
        manifest_path.write_text(json.dumps(planned, indent=2), encoding="utf-8")
        if prepared is None:
            return SensorSimResult(
                backend=self.name(),
                success=False,
                artifacts={"execution_plan": manifest_path},
                message=preparation_error or "Failed to prepare HELIOS execution.",
            )
        base_artifacts = {"execution_plan": manifest_path}
        if prepared.generated_survey_path is not None:
            base_artifacts["generated_survey"] = prepared.generated_survey_path
        if prepared.survey_mapping_metadata is not None:
            mapping_metadata_path = helios_output / "survey_mapping_metadata.json"
            mapping_metadata_path.write_text(
                json.dumps(prepared.survey_mapping_metadata, indent=2),
                encoding="utf-8",
            )
            base_artifacts["survey_mapping_metadata"] = mapping_metadata_path

        if not execute:
            return SensorSimResult(
                backend=self.name(),
                success=True,
                artifacts=base_artifacts,
                message="HELIOS execution plan generated only (execute_helios=false).",
            )

        prepared.output_root.mkdir(parents=True, exist_ok=True)
        process = subprocess.run(  # noqa: S603
            prepared.command,
            cwd=str(prepared.cwd),
            text=True,
            capture_output=True,
            check=False,
        )
        stdout_path = helios_output / "stdout.log"
        stderr_path = helios_output / "stderr.log"
        stdout_path.write_text(process.stdout, encoding="utf-8")
        stderr_path.write_text(process.stderr, encoding="utf-8")
        if process.returncode != 0:
            failure_artifacts = dict(base_artifacts)
            failure_artifacts["stdout"] = stdout_path
            failure_artifacts["stderr"] = stderr_path
            return SensorSimResult(
                backend=self.name(),
                success=False,
                artifacts=failure_artifacts,
                message=f"HELIOS command failed with exit code {process.returncode}.",
            )

        detected_output = self._extract_output_dir_from_logs(process.stdout, process.stderr)
        if detected_output is None:
            detected_output = prepared.output_root
        elif prepared.runtime == "docker" and prepared.host_root and prepared.container_root:
            detected_output = self._container_to_host_path(
                detected_output,
                host_root=prepared.host_root,
                container_root=prepared.container_root,
            )
        if not detected_output.is_absolute():
            detected_output = (prepared.cwd / detected_output).resolve()

        output_manifest_path = helios_output / "helios_output_manifest.json"
        artifacts, metrics, output_manifest = self._build_output_summary(
            root_dir=detected_output,
            output_dir_manifest=output_manifest_path,
        )
        output_manifest["detected_output_dir"] = str(detected_output)
        output_manifest["return_code"] = process.returncode
        output_manifest["runtime"] = prepared.runtime
        output_manifest_path.write_text(json.dumps(output_manifest, indent=2), encoding="utf-8")
        artifacts.update(base_artifacts)
        artifacts["stdout"] = stdout_path
        artifacts["stderr"] = stderr_path

        return SensorSimResult(
            backend=self.name(),
            success=True,
            artifacts=artifacts,
            metrics=metrics,
            message="HELIOS adapter completed.",
        )
