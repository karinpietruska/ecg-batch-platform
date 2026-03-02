#!/usr/bin/env bash
# Convenience wrapper to run the Layer 0 orchestrator from the host.
# Usage:
#   ./scripts/run_orchestrator.sh
# or:
#   RUN_DATE=2026-03-15 RECORD_IDS=100,101 ./scripts/run_orchestrator.sh

set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3}"
"$PYTHON" orchestrator/main.py

