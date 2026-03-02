#!/usr/bin/env bash
# Orchestrator CLI tests
# Run from project root. Prerequisites: docker compose up -d postgres minio minio-bootstrap

set -euo pipefail

cd "$(dirname "$0")/.."

RUN_DATE_TEST="2026-03-15"

echo "=== Orchestrator tests ==="

echo
echo "=== 0. Ensure infra is up (postgres + minio + bucket) ==="
docker compose up -d postgres minio minio-bootstrap

echo
echo "=== 1. Default run (no RUN_ID / RUN_DATE) ==="
unset RUN_ID RUN_DATE RECORD_IDS RECORD_RANGE RECORD_LIMIT
python3 orchestrator/main.py

echo "1.1 Check runs and service_runs for latest run_id"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, run_date, status
FROM runs
ORDER BY created_at DESC
LIMIT 3;"

docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, service, status, notes
FROM service_runs
ORDER BY ended_at DESC
LIMIT 6;"

echo
echo "=== 2. RUN_DATE only (orchestrator should honor external date) ==="
unset RUN_ID
export RUN_DATE="$RUN_DATE_TEST"
python3 orchestrator/main.py

echo "2.1 Verify that runs.run_date = $RUN_DATE_TEST and run_id starts with it"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, run_date, status
FROM runs
WHERE run_date = '$RUN_DATE_TEST'
ORDER BY created_at DESC
LIMIT 3;"

echo
echo "=== 3. Explicit RUN_ID and RUN_DATE (orchestrator should not override) ==="
export RUN_ID="${RUN_DATE_TEST}_explicit1234"
export RUN_DATE="$RUN_DATE_TEST"
python3 orchestrator/main.py

echo "3.1 Verify that runs and service_runs contain the explicit RUN_ID"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, run_date, status
FROM runs
WHERE run_id = '$RUN_ID'
ORDER BY created_at DESC
LIMIT 3;"

docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, service, status, notes
FROM service_runs
WHERE run_id = '$RUN_ID'
ORDER BY ended_at DESC;"

echo
echo "=== Done: orchestrator tests completed ==="

