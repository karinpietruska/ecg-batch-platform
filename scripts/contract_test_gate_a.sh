#!/usr/bin/env bash
# Gate A contract test:
# - Processing writes canonical processed RR artifacts.
# - Aggregation discovers canonical processed artifacts only.
# - Aggregation fails with exit code 1 when no canonical processed RR artifacts exist for run_id.

set -euo pipefail

cd "$(dirname "$0")/.."

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ID_OK="${RUN_ID_OK:-${RUN_DATE}_gateA_ok_${STAMP}}"
RUN_ID_FAIL="${RUN_ID_FAIL:-${RUN_DATE}_gateA_fail_${STAMP}}"

echo "=== Gate A contract test ==="
echo "RUN_DATE=$RUN_DATE"
echo "RUN_ID_OK=$RUN_ID_OK"
echo "RUN_ID_FAIL=$RUN_ID_FAIL"

echo
echo "=== 0) Ensure infra ==="
docker compose up -d postgres minio minio-bootstrap

echo
echo "=== 0.1) Build updated service images (processing + aggregation) ==="
docker compose build processing aggregation

echo
echo "=== 1) Positive path: ingestion -> processing -> aggregation ==="
RUN_ID="$RUN_ID_OK" RUN_DATE="$RUN_DATE" RECORD_IDS=100 docker compose run --rm ingestion
RUN_ID="$RUN_ID_OK" RUN_DATE="$RUN_DATE" docker compose run --rm processing
RUN_ID="$RUN_ID_OK" RUN_DATE="$RUN_DATE" docker compose run --rm aggregation

echo
echo "=== 1.1) Verify canonical processed artifacts exist ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT layer, artifact_type, schema_ver, record_id, uri
FROM artifacts
WHERE run_id = '$RUN_ID_OK'
  AND layer = 'processed'
  AND artifact_type = 'rr_intervals_v1'
  AND schema_ver = 'rr_intervals_v1'
ORDER BY record_id;"

CANON_COUNT=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT COUNT(*)
FROM artifacts
WHERE run_id = '$RUN_ID_OK'
  AND layer = 'processed'
  AND artifact_type = 'rr_intervals_v1'
  AND schema_ver = 'rr_intervals_v1';")

if [[ "${CANON_COUNT// /}" == "0" ]]; then
  echo "FAIL: No canonical processed rr_intervals_v1 artifacts found for $RUN_ID_OK"
  exit 1
fi

echo
echo "=== 1.2) Verify legacy processing RR artifacts are not written ==="
LEGACY_COUNT=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT COUNT(*)
FROM artifacts
WHERE run_id = '$RUN_ID_OK'
  AND layer = 'processing'
  AND artifact_type IN ('rr_intervals', 'rr_intervals_v1');")

if [[ "${LEGACY_COUNT// /}" != "0" ]]; then
  echo "FAIL: Found legacy processing RR artifacts for $RUN_ID_OK"
  exit 1
fi

echo
echo "=== 2) Negative path: no processed RR artifacts => aggregation must fail (exit 1) ==="
RUN_ID="$RUN_ID_FAIL" RUN_DATE="$RUN_DATE" RECORD_IDS=100 docker compose run --rm ingestion

set +e
RUN_ID="$RUN_ID_FAIL" RUN_DATE="$RUN_DATE" docker compose run --rm aggregation
AGG_EXIT_CODE=$?
set -e

if [[ "$AGG_EXIT_CODE" -eq 0 ]]; then
  echo "FAIL: Aggregation unexpectedly succeeded without canonical processed RR artifacts"
  exit 1
fi

if [[ "$AGG_EXIT_CODE" -ne 1 ]]; then
  echo "FAIL: Aggregation failed with unexpected exit code $AGG_EXIT_CODE (expected 1)"
  exit 1
fi

echo "PASS: Aggregation failed with exit code 1 as expected when no canonical processed RR artifacts exist"

echo
echo "=== 2.1) Verify failed service_run status for negative path ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, service, status, notes
FROM service_runs
WHERE run_id = '$RUN_ID_FAIL' AND service = 'aggregation';"

ENDED_AT_NULL_COUNT=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT COUNT(*)
FROM service_runs
WHERE run_id = '$RUN_ID_FAIL'
  AND service = 'aggregation'
  AND status = 'failed'
  AND ended_at IS NULL;")

if [[ "${ENDED_AT_NULL_COUNT// /}" != "0" ]]; then
  echo "FAIL: aggregation failed row has ended_at IS NULL for $RUN_ID_FAIL"
  exit 1
fi

echo
echo "Gate A contract test passed."

