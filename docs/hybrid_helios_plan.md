# Hybrid HELIOS Plan

This file is now a secondary index.

The active Sensor Sim master plan is:

- `/Users/seongcheoljeong/Documents/Test/docs/sensor_sim_master_plan.md`

## HELIOS Role Inside The Master Plan

`HELIOS++` remains the preferred high-fidelity lidar backend inside the broader Sensor Sim roadmap.

Its role is:

1. scene-level ray tracing for lidar
2. high-fidelity scan generation
3. optional hybrid execution alongside the native local path

## Boundary Definition

### HELIOS side

- scene loading
- ray casting
- raw hit generation
- scan-path-driven point cloud generation

### Local side

- typed sensor config/schema
- artifact normalization
- intensity/ground-truth enrichment
- runtime orchestration and fallback
- renderer/backend integration

## Execution Modes

- `HELIOS_ONLY`
- `NATIVE_ONLY`
- `HYBRID_AUTO`

## Current Priority

The current repository priority is no longer generic HELIOS integration first.

The order is:

1. Sensor config/schema foundation
2. Camera feature expansion
3. Lidar feature expansion with HELIOS adapter path
4. Radar feature expansion
5. Ground truth and coverage metrics
6. Runtime consolidation

## HELIOS-Specific Risks

- contract drift between HELIOS output and local schema
- runtime portability across local and Linux runner environments
- over-coupling to HELIOS-specific file formats

## Mitigation

- freeze canonical local artifact schema first
- keep HELIOS behind adapter boundaries
- preserve local native fallback for fast iteration
