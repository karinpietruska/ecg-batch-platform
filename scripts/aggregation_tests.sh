#!/usr/bin/env bash
# Aggregation service – CLI test commands
# Run from project root. Prerequisites: docker compose up -d postgres minio

set -e
cd "$(dirname "$0")/.."

RUN_ID_E2E="2026-03-10_e2e_twofile"
RUN_ID_INGEST_ONLY="2026-03-10_ingest_only"
RUN_ID_EMPTY="2026-03-10_empty_run"
RUN_ID_INVALID_FILTER="2026-03-10_invalid_filter"
RUN_DATE="2026-03-10"

echo "=== 1. Full pipeline (ingestion → processing → aggregation) ==="
export RUN_ID="$RUN_ID_E2E" RUN_DATE="$RUN_DATE" RECORD_IDS=100,101 DEV_SLICE_SECONDS=15
docker compose run --rm ingestion
docker compose run --rm processing
docker compose run --rm aggregation

echo "=== 1. Verify DB after full pipeline ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT run_id, run_date, status FROM runs WHERE run_id = '$RUN_ID_E2E';"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT run_id, record_id, layer, artifact_type, uri FROM artifacts WHERE run_id = '$RUN_ID_E2E' ORDER BY layer, record_id;"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT record_id, mean_rr_ms, sdnn_ms, rmssd_ms, pnn50, n_rr FROM features_metrics WHERE run_id = '$RUN_ID_E2E' ORDER BY record_id;"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT run_id, service, status, notes FROM service_runs WHERE run_id = '$RUN_ID_E2E' ORDER BY service;"

echo "=== 2. Idempotency: run aggregation again (expect skip) ==="
RUN_ID="$RUN_ID_E2E" RUN_DATE="$RUN_DATE" docker compose run --rm aggregation

echo "=== 2. Idempotency: run with AGG_OVERWRITE=true (expect full run) ==="
RUN_ID="$RUN_ID_E2E" RUN_DATE="$RUN_DATE" AGG_OVERWRITE=true docker compose run --rm aggregation

echo "=== 2. Idempotency: run again without overwrite (expect skip) ==="
RUN_ID="$RUN_ID_E2E" RUN_DATE="$RUN_DATE" docker compose run --rm aggregation

echo "=== 3.1 Fail mode: invalid RECORD_IDS (expect exit 1, aggregation_no_records_after_filter) ==="
# Use a run that has processing but no aggregation yet, so filter is applied and fails
RUN_ID="$RUN_ID_INVALID_FILTER" RUN_DATE="$RUN_DATE" RECORD_IDS=100,101 DEV_SLICE_SECONDS=15 docker compose run --rm ingestion
RUN_ID="$RUN_ID_INVALID_FILTER" RUN_DATE="$RUN_DATE" docker compose run --rm processing
RUN_ID="$RUN_ID_INVALID_FILTER" RUN_DATE="$RUN_DATE" RECORD_IDS=does_not_exist docker compose run --rm aggregation || true
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT run_id, service, status, notes FROM service_runs WHERE run_id = '$RUN_ID_INVALID_FILTER' AND service = 'aggregation';"

echo "=== 3.3 Fail mode: run only ingested (no processing), then aggregation (expect no_processing_records, exit 0) ==="
RUN_ID="$RUN_ID_INGEST_ONLY" RUN_DATE="$RUN_DATE" RECORD_IDS=100,101 DEV_SLICE_SECONDS=15 docker compose run --rm ingestion
# No RECORD_IDS so aggregation sees no filter and discovers no processing artifacts
RUN_ID="$RUN_ID_INGEST_ONLY" RUN_DATE="$RUN_DATE" RECORD_IDS= RECORD_RANGE= RECORD_LIMIT= docker compose run --rm aggregation
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT run_id, service, status, notes FROM service_runs WHERE run_id = '$RUN_ID_INGEST_ONLY' AND service = 'aggregation';"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT COUNT(*) AS features_rows FROM features_metrics WHERE run_id = '$RUN_ID_INGEST_ONLY';"

echo "=== 4. Empty run: insert run row, run aggregation (expect no_processing_records, exit 0) ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "INSERT INTO runs (run_id, run_date, status) VALUES ('$RUN_ID_EMPTY', '$RUN_DATE', 'completed') ON CONFLICT (run_id) DO NOTHING;"
# No RECORD_IDS so we get "no processing artifacts" not "filter didn't match"
RUN_ID="$RUN_ID_EMPTY" RUN_DATE="$RUN_DATE" RECORD_IDS= RECORD_RANGE= RECORD_LIMIT= docker compose run --rm aggregation
docker compose exec postgres psql -U ecg -d ecg_metadata -c "SELECT run_id, service, status, notes FROM service_runs WHERE run_id = '$RUN_ID_EMPTY' AND service = 'aggregation';"

echo "=== 5. Artifact consistency (run after full pipeline; counts should match, distinct_uris=1) ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT r.run_id,
  (SELECT COUNT(*) FROM artifacts a WHERE a.run_id = r.run_id AND a.layer = 'aggregation' AND a.artifact_type = 'features') AS agg_artifacts,
  (SELECT COUNT(*) FROM features_metrics f WHERE f.run_id = r.run_id) AS features_rows
FROM runs r WHERE r.run_id = '$RUN_ID_E2E';"
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT run_id, COUNT(DISTINCT uri) AS distinct_uris FROM artifacts WHERE run_id = '$RUN_ID_E2E' AND layer = 'aggregation' AND artifact_type = 'features' GROUP BY run_id;"

echo "=== Done ==="
