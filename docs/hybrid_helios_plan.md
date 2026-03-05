# Hybrid HELIOS Plan

## Objective

Deliver feature coverage fast by combining:

- `HELIOS` for scene-level ray tracing and core LiDAR simulation.
- `Local modules` for project-specific sensor physics enhancements.

## Boundary definition

### HELIOS side

- Asset/scene loading.
- Ray casting and raw hit generation.
- Baseline point cloud generation.

### Local side

- Sensor profile conversion and config management.
- Distortion/noise/motion-compensation extensions.
- Runtime orchestration, fallback, and artifact standardization.

## Execution modes

- `HELIOS_ONLY`: run HELIOS adapter only.
- `NATIVE_ONLY`: run local simulation only.
- `HYBRID_AUTO`: HELIOS first, then local enhancement; fallback to local-only on HELIOS failure.

## Milestones

1. Interface baseline (done in this repository skeleton).
2. Real HELIOS process contract integration.
3. Raw output parser and schema normalization.
4. Physics enhancement chain (camera geometry, distortion, radar/lidar noise).
5. Performance pass and validation scenarios.

## Main hurdles and mitigation

- HELIOS I/O contract drift:
  - Mitigation: freeze adapter schema version and add parser contract tests.
- Native/HELIOS output mismatch:
  - Mitigation: define canonical artifact schema with explicit metadata version.
- Platform/runtime variance:
  - Mitigation: containerized HELIOS execution profile and smoke tests.
- Over-forking risk:
  - Mitigation: keep HELIOS as external module; patch locally in adapter layer first.
