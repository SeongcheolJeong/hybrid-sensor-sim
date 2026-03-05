from __future__ import annotations

from abc import ABC, abstractmethod

from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult


class SensorBackend(ABC):
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def simulate(self, request: SensorSimRequest) -> SensorSimResult:
        raise NotImplementedError

