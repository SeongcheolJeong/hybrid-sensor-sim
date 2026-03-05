from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest


class HybridOrchestratorTests(unittest.TestCase):
    def _request(self, root: Path, execute_helios: bool = False) -> SensorSimRequest:
        scenario = root / "scenario.json"
        scenario.write_text("{}", encoding="utf-8")
        output = root / "out"
        return SensorSimRequest(
            scenario_path=scenario,
            output_dir=output,
            options={"execute_helios": execute_helios},
        )

    def test_helios_only_fails_without_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            req = self._request(root)
            orch = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=root / "missing_helios"),
                native=NativePhysicsBackend(),
            )
            result = orch.run(req, BackendMode.HELIOS_ONLY)
            self.assertFalse(result.success)
            self.assertEqual(result.backend, "helios")

    def test_hybrid_fallbacks_to_native_when_helios_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            req = self._request(root)
            orch = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=root / "missing_helios"),
                native=NativePhysicsBackend(),
            )
            result = orch.run(req, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertEqual(result.backend, "native_physics")
            self.assertIn("fallback", result.message.lower())

    def test_hybrid_uses_helios_then_native_enhancement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            fake_helios.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_helios.chmod(0o755)

            req = self._request(root, execute_helios=True)
            req.options["helios_command"] = [str(fake_helios)]

            orch = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orch.run(req, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertEqual(result.backend, "hybrid(helios+native_physics)")
            self.assertIn("hybrid_physics", result.artifacts)


if __name__ == "__main__":
    unittest.main()

