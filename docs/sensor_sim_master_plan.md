# Sensor Sim Master Plan

## Goal

Build a `Sensor Sim` stack whose minimum usable feature set is above the baseline described in the Applied Intuition v1.64 Sensor Sim docs, while keeping the implementation open, portable, and incrementally testable.

This plan explicitly prioritizes:

1. `Feature coverage` over repeated validation-only work.
2. `Core sensor blocks` over peripheral polish.
3. `Migration-ready architecture` over one-off demos.

## Success Definition

We need a realistic definition of "Applied Intuition 보다 높게".

For this repository, the minimum target is:

1. Match or exceed `core parameter surface` for camera, lidar, and radar.
2. Match or exceed `raw output + metadata + ground truth` capabilities for core workflows.
3. Exceed Applied Intuition in `open integration`:
   - backend-agnostic runtime contracts
   - portable launcher templates
   - renderer/backend decoupling
   - externally inspectable manifests and artifacts
4. Exceed Applied Intuition in `hybridization`:
   - local native physics path
   - external renderer/runtime path
   - external reference engine migration path

Important constraint:

- We can exceed Applied Intuition first in `openness, portability, inspectability`, while sensor physics fidelity catches up in phases.
- We should not claim full physical superiority before calibration and scenario-level benchmarking exist.

## Applied Intuition Scope Summary

Primary local references:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensor_types/camera/camera_parameter_summary/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensor_types/camera/camera_parameter_reference/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensor_types/lidar/lidar_parameter_summary/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensor_types/lidar/lidar_parameter_reference/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensor_types/radar/radar_parameter_summary/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensor_types/radar/radar_parameter_reference/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensors_overview/sensor_behaviors/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/sensors_overview/ground_truth/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/tutorials/configuring_sensor_sim_sensors/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/tutorials/modeling_a_radar_using_a_datasheet/index.clean.md`
- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/20_Knowledge/Sim/AppliedDocs_v1.64/manual/v1.64/docs/sensor_sim/tutorials/sensor_coverage_metrics/index.clean.md`

### Common Blocks In Applied Intuition

- Sensor mount and transform hierarchy
- Sensor behaviors:
  - `point_at`
  - `continuous_motion`
- Sensor outputs:
  - images
  - point clouds
  - ranges
  - raw streams
  - sensor output lists
- Ground truth:
  - semantic class
  - actor id
  - component id
  - material class / material UUID
  - base map / procedural map / lane marking ids
- Coverage metrics:
  - pixels on target
  - lidar points on target
  - radar detections on target
  - blindspot / overlap / occlusion style analysis

### Camera Parameter Surface In Applied Intuition

- Geometry and optics:
  - projection: `RECTILINEAR`, `EQUIDISTANT`, `ORTHOGRAPHIC`
  - field of view
  - focal length
  - intrinsic parameters `fx fy cx cy`
  - optical cropping
  - radial and OpenCV distortion
  - projection field URI
  - cubemap controls
- Sensor and image chain:
  - resolution
  - pixel size
  - sensor size
  - sensor type: visible / depth / semantic / NIR / RGB-IR / LWIR / optical flow / rigid displacement
  - color depth
  - ISO
  - quantum efficiency
  - dynamic range
  - shutter speed
  - full well capacity
  - CCD/CMOS
  - CFA + demosaic
- Noise and post-processing:
  - readout noise
  - DSNU / PRNU
  - shot noise
  - bloom
  - gamma
  - white balance
  - color space
  - exposure / auto exposure
  - black level
- Temporal and weather artifacts:
  - rolling shutter
  - motion blur
  - dirt / fog / droplets
- Rendering fidelity controls:
  - lighting
  - shadows
  - super sampling
  - view distance
  - hybrid / ray-traced rendering options

### Lidar Parameter Surface In Applied Intuition

- Emitter geometry:
  - `source_angles`
  - per-channel loss
  - beam divergence
  - source variance
  - peak power
  - center wavelength
- Scan generation:
  - `scan_type`: `SPIN`, `FLASH`, `CUSTOM`
  - `scan_field`
  - `scan_field_offset`
  - `spin_direction`
  - `scan_path`
  - `multi_scan_path`
  - drift terms
  - scan path file URI
- Detector and estimator:
  - resolution
  - min/max range
  - aperture area
  - range resolution
  - range discrimination
  - optical passband
  - ambient rejection
  - saturation limit
  - minimum SNR
  - dynamic range
  - noise floor
  - return mode / return count
- Intensity and outputs:
  - intensity units
  - quantization
  - scaled reflectivity mapping
  - point cloud output fields
  - include ground-truth points
  - raw packet stream / UDP config
- Physics and fidelity:
  - ambient
  - shot noise
  - ideal returns
  - translucency
  - material source type
  - backscatter
  - subsampling
  - hardware/software ray tracing
  - multipath bounces
  - motion distortion fidelity
  - fog extinction scaling

### Radar Parameter Surface In Applied Intuition

- Core geometry:
  - FoV az/el
  - angular resolution
  - angular quantization
  - frame rate
- RF system:
  - transmit power
  - radiometric calibration factor
  - center frequency
  - range / range resolution / range quantization
  - velocity / velocity resolution / velocity quantization
  - RF bandwidth
  - ADC / chirp / PRI
- Antenna and directivity:
  - antenna definitions
  - parametric beams
  - directivity tables
  - polarization
  - transmit/receive array geometry
- Detection and estimation:
  - noise variance
  - minimum SNR
  - probability of detection
  - probability of false alarm
  - target detectability at range/RCS
  - max detections
  - range / velocity / azimuth / elevation accuracy
  - per-region accuracy tuning
- Tracking and outputs:
  - points vs tracks
  - semantic and image outputs
- Fidelity:
  - ray density
  - multipath
  - multipath bounce count
  - sub-ray angular resolution
  - max ray hits
  - coherence factor
  - hardware/software ray tracing
  - adaptive sampling
  - cavity model
  - micro-doppler

## Current Repository Implementation

Primary code references:

- `/Users/seongcheoljeong/Documents/Test/src/hybrid_sensor_sim/physics/camera.py`
- `/Users/seongcheoljeong/Documents/Test/src/hybrid_sensor_sim/backends/native_physics.py`
- `/Users/seongcheoljeong/Documents/Test/src/hybrid_sensor_sim/renderers/playback_contract.py`
- `/Users/seongcheoljeong/Documents/Test/src/hybrid_sensor_sim/renderers/runtime_executor.py`

### Implemented Now

- Common:
  - playback contract generation
  - backend frame manifest generation
  - ingestion profile generation
  - backend sensor bundle summary generation
  - backend run manifest generation
  - backend runner request and direct command generation
  - launcher template generation
  - wrapper-first renderer/backend command flow
  - wrapper-level bundle summary consumption for backend-debug/runtime inspection
  - trajectory-aware extrinsics propagation
- Camera:
  - pinhole-style projection flow
  - Brown-Conrady distortion
  - intrinsics / extrinsics support
  - trajectory sweep preview
  - reference-point based coordinate adjustment
- Lidar:
  - simple noisy point cloud post-process
  - dropout
  - basic motion compensation
  - trajectory sweep preview
  - extrinsics from options or trajectory
- Radar:
  - target list post-process from point cloud
  - range / FoV filtering
  - angle / range / velocity noise
  - simple clutter / false alarms
  - typed detector/system/estimator/tracking schema
  - antenna HPBW gain model
  - datasheet-style detectability model:
    - `target_detectability.target.{range,radar_cross_section}`
    - `probability_detection`
    - `minimum_snr_db`
    - `probability_false_alarm`
  - global/per-region accuracy surface
  - track output mode:
    - same-actor detection grouping
    - incoherent RCS sum
    - geometric-center range/angle track state
    - multipath source summary on tracks
    - trajectory-sweep continuity:
      - persistent track IDs
      - track history length / age
      - reassociation summary
      - coast / termination state
  - path-type-aware multipath approximation:
    - `fidelity.multipath`
    - `fidelity.multipath_bounces`
    - `coherence_factor`
    - `forward / reverse / retroreflection / cavity retroreflection`
    - `GROUND_TRUTH_HIT_INDEX / GROUND_TRUTH_LAST_BOUNCE_INDEX` semantics aligned to Applied docs
    - multipath ghost detections with bounce/path metadata
  - micro-doppler velocity hook:
    - `enable_micro_doppler`
  - directivity/adaptive sampling surface:
    - `antenna_definitions[].directivity_az_el_cuts`
    - `fidelity.raytracing.mode`
    - `adaptive_sampling_params.default_min_rays_per_wavelength`
    - `adaptive_sampling_params.targets[].actor_id`
    - target-level sampling/direction metadata in preview artifacts
  - trajectory sweep preview
  - ego-velocity-based radial velocity estimation

### Not Implemented Or Still Weak

- Common:
  - sensor behavior engine
  - transform forest / mount hierarchy
  - unified sensor output list schema
  - full ground truth propagation
  - coverage metrics
- Camera:
  - equidistant / orthographic / projection field models
  - rolling shutter exposure model
  - CFA / demosaic / sensor chain
  - exposure / gain / white balance / color space model
  - weather/shroud artifacts
  - depth / semantic / optical flow modalities
  - lens flare / bloom / chromatic aberration
- Lidar:
  - scan path and channel array modeling
  - spin/flash/custom scan modes
  - beam divergence and per-channel calibration
  - intensity units and return modes
  - weather attenuation and backscatter
  - material-aware reflectivity / source wavelength
  - multi-return and multipath
  - raw packet / structured output formats
- Radar:
  - richer full directivity-table support beyond current az/el cut approximation
  - deeper datasheet calibration against real devices
  - track lifecycle/filtering beyond one-frame track projection
  - geometry-aware multipath physics beyond current synthetic stage-1 model
  - stronger adaptive sampling semantics beyond current target-density heuristic
  - richer micro-doppler beyond current velocity hook
  - hardware ray-tracing equivalent path

## Gap Assessment

### Feature Priority Levels

- `P0`: missing foundation that blocks many features
- `P1`: sensor core behavior required for practical use
- `P2`: fidelity or modality expansion that materially changes outcomes
- `P3`: optimization, convenience, or advanced realism

### Gap Table

| Block | Current Level | Target Level | Priority |
| --- | --- | --- | --- |
| Sensor config schema | typed camera/lidar/radar/renderer/behavior config layer implemented; validation still thin | typed sensor model schema with validation | P0 |
| Sensor behavior engine | first behavior now applied for camera/lidar/radar; `point_at` and `continuous_motion` update resolved extrinsics in preview/sweep paths | `point_at`, `continuous_motion`, transform updates | P1 |
| Camera geometry families | `pinhole` + `rectilinear` + `equidistant` + `orthographic` implemented in local physics path | rectilinear + equidistant + orthographic + projection field adapter | P1 |
| Camera temporal model | rolling shutter timing + trajectory pose-distortion implemented; behavior-driven pose updates now connected | rolling shutter + exposure sampling | P1 |
| Camera image chain | depth + semantic + visible image-chain preview implemented; vignetting/lens flare/spot blur added, richer optical artifacts still missing | noise, gain, white balance, depth/semantic variants | P2 |
| Lidar scan generation | source angles + scan field + scan path + multi-scan path metadata/filtering implemented in local preview path | source angles + scan path + scan type engine | P1 |
| Lidar signal/intensity | reflectivity/SNR/intensity units/returns/weather/emitter/channel profile implemented in local preview path | reflectivity, SNR/intensity units, returns, weather | P1 |
| Lidar multipath/material model | geometry-aware multipath and shared-channel profile are implemented locally; material UUID remains synthetic/fallback | HELIOS-backed or local hybrid path | P2 |
| Radar beam/detectability | typed HPBW + az/el directivity cuts + detectability + false alarm calibration implemented | richer full directivity tables + calibration | P1 |
| Radar multipath/tracking | track output + region accuracies + synthetic multipath implemented; adaptive sampling/directivity exposed | geometry-aware multipath + stronger tracker | P2 |
| Ground truth | camera/lidar/radar ground-truth surface exposed; lidar/radar actor/semantic labels now emitted in local preview path | semantic/material/component labels in outputs | P1 |
| Coverage metrics | camera/lidar/radar per-target coverage summary plus combined blindspot/overlap artifact implemented | camera/lidar/radar target coverage stats | P1 |
| Runtime integration | strong | keep extending, do not make this the primary bottleneck | P2 |

## External Reference Strategy

We should not treat every external repository the same way.

There are three usage modes:

1. `Direct migration`
   - lift algorithms, data structures, or adapters with light modification
2. `Hybrid integration`
   - execute an external backend and normalize its outputs locally
3. `Behavioral reference`
   - copy model ideas and parameter semantics, but implement locally

### Reference Map

| Reference | Role | Usage Mode | Why |
| --- | --- | --- | --- |
| [CARLA](https://carla.org/) / [CARLA sensor docs](https://carla.readthedocs.io/en/latest/ref_sensors/) | camera/radar/semantic modality reference | Behavioral reference + partial integration | mature sensor modality surface and output semantics |
| [AWSIM](https://github.com/tier4/AWSIM) | renderer/runtime and Autoware-facing integration | Hybrid integration | practical rendered validation path and Autoware-aligned workflow |
| [HELIOS++](https://github.com/3dgeo-heidelberg/helios) | lidar scan/ray tracing engine | Hybrid integration + selective migration | strongest external lidar simulation candidate |
| [Autoware](https://github.com/autowarefoundation/autoware) | message contracts, frame conventions, stack expectations | Behavioral reference | useful for output compatibility and runtime contracts |
| [openpilot](https://github.com/commaai/openpilot) | camera calibration and temporal consistency ideas | Behavioral reference only | useful for calibration/extrinsics thinking, not for direct Sensor Sim migration |
| [CARLA simulator repo](https://github.com/carla-simulator/carla) | semantic lidar/radar/camera implementation reference | Behavioral reference | helps map feature coverage and output conventions |

### Per-Block Migration Decisions

#### Camera

- Primary references:
  - Applied Intuition camera docs
  - CARLA sensor modalities and semantic/depth outputs
  - openpilot calibration pipeline as a secondary reference
- Strategy:
  - implement geometry and temporal model locally
  - use CARLA output semantics as a reference for depth/semantic/optical-flow style outputs
  - do not migrate openpilot sensor code directly; only borrow calibration and timing ideas

#### Lidar

- Primary references:
  - Applied Intuition lidar docs
  - HELIOS++ engine
  - AWSIM/Tier4 sensor integration paths
  - CARLA lidar output semantics as a secondary baseline
- Strategy:
  - keep native local lidar path for fast preview and contract testing
  - add HELIOS adapter as the high-fidelity scan and ray-tracing backend
  - normalize both into one canonical point-cloud artifact schema
  - use AWSIM for rendered/runtime inspection, not as the main lidar physics source

#### Radar

- Primary references:
  - Applied Intuition radar docs
  - CARLA radar baseline
  - local implementation for detectability and antenna models
- Strategy:
  - do not rely on CARLA alone for radar parity; CARLA is too thin for full Applied-like radar modeling
  - build radar physics locally around:
    - antenna beam model
    - detectability model
    - datasheet mapping
    - accuracy regions
    - multipath progression
  - use CARLA outputs mainly for interoperability tests and visualization

#### Ground Truth And Coverage

- Primary references:
  - Applied Intuition ground truth and coverage metrics docs
  - CARLA semantic outputs
  - Autoware-compatible metadata packaging
- Strategy:
  - create local canonical ground-truth field enum first
  - map sensor outputs to actor/semantic/material IDs
  - then compute per-target sensor coverage metrics from canonical outputs

## Main Hurdles And Solutions

### Hurdle 1: Parameter Explosion

Problem:

- Applied Intuition exposes a much larger parameter surface than our current `options` dictionary can safely support.

Solution:

- Introduce typed sensor config models:
  - `CameraModelConfig`
  - `LidarModelConfig`
  - `RadarModelConfig`
  - `SensorBehaviorConfig`
- Keep backward-compatible `options` ingestion through a translation layer.

### Hurdle 2: Physics Quality Differs By Sensor

Problem:

- Camera, lidar, and radar need very different approaches.
- Reusing one backend design for all three will produce weak fidelity.

Solution:

- Use per-sensor execution strategies:
  - camera: local physics + renderer outputs
  - lidar: local fast path + HELIOS high-fidelity path
  - radar: local physics-first path with later renderer support

### Hurdle 3: External Repos Are Heavy And Heterogeneous

Problem:

- CARLA, AWSIM, and HELIOS have different languages, data models, and runtime assumptions.

Solution:

- Never embed them directly into the core domain model.
- Build adapters around a stable local canonical schema:
  - `sensor_setup_manifest`
  - `frame_input_manifest`
  - `backend_ingestion_profile`
  - future `sensor_output_manifest`

### Hurdle 4: Renderer Work Can Consume All Time

Problem:

- Runtime/launcher/integration work is useful but can starve feature implementation.

Solution:

- Cap renderer/runtime work to support milestones only.
- Feature completion order must always be:
  - sensor model
  - output schema
  - ground truth
  - then renderer validation

### Hurdle 5: Hard To Claim "Above Applied" Without Benchmarks

Problem:

- Without calibration scenarios, the phrase "better than Applied" becomes ungrounded.

Solution:

- Define superiority first in:
  - openness
  - inspectability
  - backend portability
  - configuration transparency
- Define parity or superiority in physics only after scenario calibration benchmarks exist.

## Master Execution Plan

### Phase 0: Canonical Sensor Model Foundation

Goal:

- Replace ad hoc sensor option sprawl with stable config/schema and artifact contracts.

Deliverables:

- typed config classes for camera/lidar/radar/behaviors
- parser from existing `options`
- versioned output schema for sensor artifacts
- canonical ground-truth field enum

Why first:

- Every later feature otherwise becomes duplicated option parsing and fragile JSON shape drift.

### Phase 1: Camera Core Feature Expansion

Goal:

- Move from simple pinhole preview to real camera model families and temporal behavior.

Deliverables:

- geometry families:
  - rectilinear
  - equidistant
  - orthographic
- distortion families:
  - Brown-Conrady
  - extended rational/radial distortion path
- rolling shutter model
- exposure sampling hooks
- depth and semantic image output modes
- camera behavior integration with mounts

Current status:

- implemented:
  - typed camera config path
  - rectilinear/equidistant/orthographic projection modes
  - depth preview output path
  - semantic preview output path with legacy/granular class version support
  - visible image-chain preview with exposure/gain/white-balance/readout-noise/fixed-pattern-noise
  - lens artifact preview with vignetting / lens flare / spot blur radius
  - rolling shutter timing metadata
  - rolling shutter pose-distortion using HELIOS trajectory poses
- next missing items:
  - richer optical artifacts such as chromatic aberration / lens contamination / flare ghosts
  - behavior-driven motion updates for rolling shutter

Primary references:

- Applied camera docs
- CARLA camera/depth/semantic sensor docs
- openpilot calibration concepts

### Phase 2: Lidar Core Feature Expansion

Goal:

- Move from noisy XYZ preview to real scan and signal modeling.

Deliverables:

- scan engine:
  - source angles
  - scan field
  - scan path
  - spin/flash/custom
- per-channel calibration:
  - divergence
  - losses
  - angular variance
- signal model:
  - reflectivity-aware intensity
  - return modes
  - range discrimination
  - weather attenuation/backscatter
- multi-return schema
- canonical lidar output fields with ground-truth metadata

Primary references:

- Applied lidar docs
- HELIOS++ for scan/ray path
- AWSIM and Autoware for output/runtime compatibility

Current status:

- implemented:
  - typed lidar scan config surface
  - source angle/channel assignment
  - scan field and scan field offset filtering
  - scan path and multi-scan path filtering in preview/trajectory sweep
  - structured lidar preview metadata alongside existing xyz compatibility path
  - intensity/signal output surface:
    - `REFLECTIVITY`, `REFLECTIVITY_SCALED`, `SNR`, `SNR_SCALED`, `POWER`, `LASER_CROSS_SECTION`, `GROUND_TRUTH_REFLECTIVITY`
    - `signal_photons`, `ambient_photons`, `snr`, `snr_db`, `return_id` preview fields
    - SNR-threshold based detection gating with `return_all_hits` override
  - synthetic multi-return surface:
    - `SINGLE`, `DUAL`, `MULTI` return modes
    - `max_returns`, `selection_mode`, `range_discrimination`, `range_separation_m`, `signal_decay`, `minimum_secondary_snr_db`
    - per-return `ground_truth_hit_index` and `ground_truth_last_bounce_index` preview metadata
    - Applied-style `return_count` alias and `FIRST|LAST|STRONGEST` selection ordering
  - geometry-aware multipath surface:
    - `GROUND_PLANE`, `VERTICAL_PLANE`, `HYBRID` bounce modes
    - `max_paths`, `path_signal_decay`, `minimum_path_snr_db`, `max_extra_path_length_m`
    - plane parameters: `ground_plane_height_m`, `ground_reflectivity`, `wall_plane_x_m`, `wall_reflectivity`
    - preview metadata: reflection point, bounce surface, apparent path length
  - environment/weather surface:
    - fog attenuation with `fog_density * extinction_coefficient_scale`
    - backscatter/noise returns using `backscatter_scale`
    - type-specific precipitation particle field:
      - `precipitation_type=RAIN|SNOW|HAIL`
      - `particle_density_scale`, `particle_diameter_mm`, `terminal_velocity_mps`
      - `particle_reflectivity`, `backscatter_jitter`, `field_seed`
      - precipitation attenuation folded into `weather_extinction_factor`
      - precipitation metadata emitted into preview artifacts
    - detector `probability_false_alarm` surface
  - emitter/channel surface:
    - `source_losses` and `global_source_loss`
    - `source_divergence` and `source_variance`
    - `peak_power` and range-dependent `optical_loss`
    - per-point channel radiometric metadata in preview artifacts
  - channel profile / detector profile surface:
    - `shared_channel_profile.profile_data.{file_uri,half_angle,scale}`
    - built-in `CROSS`, `GRID`, `RING` synthetic profile fallback
    - off-axis `SIDELOBE` preview returns with profile offsets and weights
    - file-backed profile ingestion for `json/csv/txt/npy` and `.exr` sidecar fallback
- next missing items:
  - stronger raytracing fidelity beyond current selection/merge approximation
  - richer rain/snow particle field calibration beyond current type-specific heuristic
  - native EXR decode beyond current sidecar fallback

### Phase 3: Radar Core Feature Expansion

Goal:

- Replace heuristic target extraction with datasheet-driven radar modeling.

Deliverables:

- antenna beam model
- datasheet-driven detectability model
- probability of detection / false alarm calibration
- range / velocity / azimuth / elevation accuracy models
- track output mode
- multipath progression:
  - stage 1: configurable synthetic multipath
  - stage 2: geometry-aware bounce model
  - stage 3: adaptive sampling and micro-doppler hooks

Primary references:

- Applied radar docs
- Applied radar datasheet tutorial
- CARLA radar output baseline

### Phase 4: Ground Truth And Coverage Metrics

Goal:

- Make outputs useful for design and analysis, not just visualization.

Deliverables:

- canonical semantic/material/component labels
- per-hit actor binding
- camera pixels on target
- lidar points on target
- radar detections on target
- overlap/blindspot/occlusion report artifacts

Current status:

- local physics path now emits per-sensor `coverage_targets` plus `sensor_coverage_summary.json`
- combined summary already reports blindspot and overlap counts
- remaining gap is deeper occlusion reasoning and richer actor/component source ingestion

Primary references:

- Applied ground truth docs
- Applied coverage metrics docs
- CARLA semantic outputs

### Phase 5: Runtime And Renderer Consolidation

Goal:

- Plug the richer Sensor Sim outputs into AWSIM/CARLA without slowing feature growth.

Deliverables:

- backend runner abstraction
- unified run manifest
- renderer/backend result packaging
- AWSIM and CARLA sample scenario playback
- Linux runner guidance and smoke scenarios

Constraint:

- This phase supports the sensor blocks; it must not become the main track again.

## Immediate Next Milestones

### Milestone A

- Implement typed config/schema layer.
- Output:
  - schema module
  - translation from legacy options
  - tests that freeze config behavior

### Milestone B

- Implement camera model v2.
- Output:
  - equidistant geometry
  - rolling shutter sampling
  - depth/semantic output artifact format
  - tests for projection and temporal behavior

### Milestone C

- Implement lidar scan engine v2.
- Output:
  - source angles
  - scan path
  - spin/custom/flash modes
  - HELIOS adapter contract

### Milestone D

- Implement radar datasheet model v2.
- Output:
  - beam model
  - detectability model
  - accuracy regions
  - track output schema

## Near-Term Work Order

1. `Schema first`
2. `Camera v2`
3. `Lidar v2`
4. `Radar v2`
5. `Ground truth + coverage`
6. `Renderer/runtime consolidation`

This ordering is intentional.

- It prevents us from spending another cycle on runtime-only improvements.
- It increases visible feature coverage fastest.
- It creates the base needed for HELIOS/AWSIM/CARLA migration without rewriting interfaces again.

## What We Will Not Do First

- More launcher polishing without new sensor capability
- More smoke-test-only work without feature expansion
- More backend wrappers if they do not unlock camera/lidar/radar coverage
- Deep performance tuning before scan/model completeness exists

## Tracking Rule

Progress should now be checked against this file first:

- `/Users/seongcheoljeong/Documents/Test/docs/sensor_sim_master_plan.md`

Secondary index:

- `/Users/seongcheoljeong/Documents/Test/docs/hybrid_helios_plan.md`

Every future task should be mapped to:

1. which phase it belongs to,
2. which sensor block it unlocks,
3. whether it increases feature coverage or only validation.
