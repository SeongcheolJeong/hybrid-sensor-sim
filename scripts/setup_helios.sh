#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
THIRD_PARTY_DIR="${ROOT_DIR}/third_party"
HELIOS_DIR="${THIRD_PARTY_DIR}/helios"

mkdir -p "${THIRD_PARTY_DIR}"

if [[ ! -d "${HELIOS_DIR}/.git" ]]; then
  git clone https://github.com/3dgeo-heidelberg/helios "${HELIOS_DIR}"
else
  echo "HELIOS already exists at ${HELIOS_DIR}; skipping clone."
fi

echo "HELIOS source path: ${HELIOS_DIR}"
echo "Next steps:"
echo "  1) cd ${HELIOS_DIR}"
echo "  2) mkdir -p build && cd build"
echo "  3) cmake .."
echo "  4) cmake --build . -j"
echo "  5) export HELIOS_BIN=<path to built executable>"
