#!/usr/bin/env bash
# Scheduled orchestrator wrapper (host-based).
# Intended to be called by cron or a CI scheduler.
#
# Example cron entry (daily at 02:00 UTC):
#   0 2 * * * cd /path/to/ecg-batch-platform && ./scripts/scheduled_orchestrator.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "REPO_ROOT=$REPO_ROOT"

# Ensure required tools are available in the cron environment.
command -v python3 || { echo "python3 not found on PATH"; exit 1; }
command -v docker || { echo "docker not found on PATH"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "docker compose not available"; exit 1; }

# Default RUN_DATE to UTC date if not provided.
RUN_DATE="${RUN_DATE:-$(date -u +%F)}"

# Logging: one file per invocation, with UTC timestamp for uniqueness.
: "${LOG_DIR:=logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="$LOG_DIR/orchestrator_${RUN_DATE}_${STAMP}.log"

EXIT_CODE=0

{
  echo "=== Scheduled orchestrator run ==="
  echo "UTC now: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "RUN_DATE=$RUN_DATE"
  echo "RECORD_IDS=${RECORD_IDS:-<unset>}"
  echo "RECORD_RANGE=${RECORD_RANGE:-<unset>}"
  echo "RECORD_LIMIT=${RECORD_LIMIT:-<unset>}"
  echo "AGG_OVERWRITE=${AGG_OVERWRITE:-<unset>}"

  RUN_DATE="$RUN_DATE" \
  RECORD_IDS="${RECORD_IDS:-}" \
  RECORD_RANGE="${RECORD_RANGE:-}" \
  RECORD_LIMIT="${RECORD_LIMIT:-}" \
  AGG_OVERWRITE="${AGG_OVERWRITE:-}" \
  ./scripts/run_orchestrator.sh || EXIT_CODE=$?

  echo "orchestrator_exit_code=$EXIT_CODE"
} >> "$LOG_FILE" 2>&1

exit "$EXIT_CODE"

