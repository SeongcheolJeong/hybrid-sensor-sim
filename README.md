# Hybrid HELIOS Sensor Sim

This repository implements a hybrid integration strategy for [HELIOS](https://github.com/3dgeo-heidelberg/helios):

- Use HELIOS as the external geometry/raycast backend.
- Keep project-specific physics improvements (noise, distortion, post-processing) in local code.
- Orchestrate both through a single runtime path with fallback behavior.

## Why hybrid

- Avoid a hard fork of HELIOS while still using its core strengths.
- Preserve flexibility for domain-specific sensor behavior upgrades.
- Keep maintenance cost lower than full in-house reimplementation.

## Structure

- `src/hybrid_sensor_sim/backends/helios_adapter.py`: external HELIOS execution adapter.
- `src/hybrid_sensor_sim/backends/native_physics.py`: local physics enhancement layer.
- `src/hybrid_sensor_sim/orchestrator.py`: mode selection and pipeline chaining.
- `docs/hybrid_helios_plan.md`: functional roadmap and risk management.
- `scripts/setup_helios.sh`: bootstrap helper for cloning/building HELIOS.

## Quick start

```bash
python3 -m hybrid_sensor_sim.cli --config configs/hybrid_sensor_sim.example.json
```

Run tests:

```bash
python3 -m unittest -q
```

## Next implementation target

- Replace placeholder HELIOS command flow with actual scene/sensor config mapping.
- Connect generated point cloud output into calibrated camera/lidar/radar post-physics stages.
