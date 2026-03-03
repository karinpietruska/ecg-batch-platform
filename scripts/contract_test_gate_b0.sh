#!/usr/bin/env bash
# Gate B0 contract test:
# - Verify rr_intervals_v1 produced by processing contains t_peak_sec.
# - Verify t_peak_sec has at least one non-null value for a controlled run.

set -euo pipefail

cd "$(dirname "$0")/.."

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ID="${RUN_ID:-${RUN_DATE}_gateB0_${STAMP}}"

echo "=== Gate B0 contract test ==="
echo "RUN_DATE=$RUN_DATE"
echo "RUN_ID=$RUN_ID"

echo
echo "=== 0) Ensure infra ==="
docker compose up -d postgres minio minio-bootstrap

echo
echo "=== 0.1) Build processing image ==="
docker compose build processing

echo
echo "=== 1) Produce processed rr_intervals_v1 artifact ==="
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS=100 docker compose run --rm ingestion
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" docker compose run --rm processing

echo
echo "=== 1.1) Fetch canonical processed RR artifact row ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT layer, artifact_type, schema_ver, record_id, uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'processed'
  AND artifact_type = 'rr_intervals_v1'
  AND schema_ver = 'rr_intervals_v1'
ORDER BY record_id;"

TARGET_URI=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'processed'
  AND artifact_type = 'rr_intervals_v1'
  AND schema_ver = 'rr_intervals_v1'
ORDER BY record_id
LIMIT 1;")

TARGET_URI="${TARGET_URI//[$'\r\n']}"

if [[ -z "$TARGET_URI" ]]; then
  echo "FAIL: No canonical processed rr_intervals_v1 artifact URI found for $RUN_ID"
  exit 1
fi

echo "TARGET_URI=$TARGET_URI"

echo
echo "=== 2) Assert t_peak_sec exists and has non-null values ==="
docker compose run --rm -e TARGET_URI="$TARGET_URI" processing python -c "
import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

endpoint = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
bucket = os.environ.get('MINIO_BUCKET', 'ecg-datalake')
key = os.environ['TARGET_URI']
ak = os.environ.get('AWS_ACCESS_KEY_ID')
sk = os.environ.get('AWS_SECRET_ACCESS_KEY')

if not ak or not sk:
    raise SystemExit('FAIL: missing AWS credentials in processing container env')

s3 = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=ak,
    aws_secret_access_key=sk,
)
obj = s3.get_object(Bucket=bucket, Key=key)
table = pq.read_table(pa.BufferReader(obj['Body'].read()))

if 't_peak_sec' not in table.schema.names:
    raise SystemExit('FAIL: t_peak_sec column missing in rr_intervals_v1')

num_rows = table.num_rows
col = table.column('t_peak_sec')
nonnull = col.length() - col.null_count

if num_rows == 0:
    raise SystemExit('FAIL: rr_intervals_v1 table has 0 rows for Gate B0 test run')
if nonnull == 0:
    raise SystemExit('FAIL: t_peak_sec exists but all values are null')

print(f'PASS: t_peak_sec present with non-null values (rows={num_rows}, nonnull={nonnull})')
"

echo
echo "Gate B0 contract test passed."

