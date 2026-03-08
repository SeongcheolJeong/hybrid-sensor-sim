from __future__ import annotations

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest, SensorSimResult


class HybridOrchestrator:
    def __init__(
        self,
        helios: HeliosAdapter | None = None,
        native: NativePhysicsBackend | None = None,
    ) -> None:
        self.helios = helios or HeliosAdapter()
        self.native = native or NativePhysicsBackend()

    def run(self, request: SensorSimRequest, mode: BackendMode) -> SensorSimResult:
        if mode == BackendMode.HELIOS_ONLY:
            return self.helios.simulate(request)

        if mode == BackendMode.NATIVE_ONLY:
            return self.native.simulate(request)

        helios_result = self.helios.simulate(request)
        if helios_result.success:
            return self.native.enhance_from_helios(request, helios_result)

        fallback = self.native.simulate(request)
        fallback.artifacts.update(
            {f"helios_failure_{key}": value for key, value in helios_result.artifacts.items()}
        )
        fallback.metrics.update(
            {f"helios_failure_{key}": value for key, value in helios_result.metrics.items()}
        )
        fallback_detail = str(fallback.message or "").strip()
        fallback.message = (
            "HELIOS failed; fallback to native simulation. "
            f"reason={helios_result.message}"
        )
        if fallback_detail:
            fallback.message = f"{fallback.message} Native result: {fallback_detail}"
        return fallback
