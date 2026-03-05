#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HELIOS_DIR="${ROOT_DIR}/third_party/helios"
IMAGE_TAG="${1:-heliosplusplus:local}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not running. Start Docker Desktop first."
  exit 1
fi

if [[ ! -f "${HELIOS_DIR}/Dockerfile" ]]; then
  echo "missing HELIOS Dockerfile at ${HELIOS_DIR}/Dockerfile"
  exit 1
fi

echo "Building HELIOS image: ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" -f "${HELIOS_DIR}/Dockerfile" "${HELIOS_DIR}"
echo "Built ${IMAGE_TAG}"
