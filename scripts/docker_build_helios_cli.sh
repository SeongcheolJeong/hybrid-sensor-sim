#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HELIOS_SRC="${ROOT_DIR}/third_party/helios"
DOCKERFILE="${ROOT_DIR}/docker/helios-cli.Dockerfile"
IMAGE_TAG="${1:-heliosplusplus:cli}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not running. Start Docker Desktop first."
  exit 1
fi

if [[ ! -d "${HELIOS_SRC}" ]]; then
  echo "missing HELIOS source at ${HELIOS_SRC}"
  exit 1
fi

if [[ ! -f "${DOCKERFILE}" ]]; then
  echo "missing dockerfile at ${DOCKERFILE}"
  exit 1
fi

echo "Building HELIOS CLI image: ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" -f "${DOCKERFILE}" "${HELIOS_SRC}"
echo "Built ${IMAGE_TAG}"
