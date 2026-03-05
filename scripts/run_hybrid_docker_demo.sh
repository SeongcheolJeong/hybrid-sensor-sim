#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-${ROOT_DIR}/configs/hybrid_sensor_sim.helios_docker.json}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not running. Start Docker Desktop first."
  exit 1
fi

cd "${ROOT_DIR}"
PYTHONPATH=src python3 -m hybrid_sensor_sim.cli --config "${CONFIG_PATH}"
