#!/usr/bin/env bash
# Gate B contract test:
# - Runs ingestion -> processing -> aggregation.
# - Verifies curated artifact registration and prefix existence.
# - Verifies t_peak_sec exists/non-null in processed RR input.
# - Verifies deterministic window count for the tested record:
#     expected_windows = floor(max(t_peak_sec)/300) + 1
# - Verifies record-level idempotency: second aggregation run with AGG_OVERWRITE=false skips.

set -euo pipefail

cd "$(dirname "$0")/.."

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ID="${RUN_ID:-${RUN_DATE}_gateB_${STAMP}}"
RECORD_ID="${RECORD_ID:-100}"

echo "=== Gate B contract test ==="
echo "RUN_DATE=$RUN_DATE"
echo "RUN_ID=$RUN_ID"
echo "RECORD_ID=$RECORD_ID"

echo
echo "=== 0) Ensure infra ==="
docker compose up -d postgres minio minio-bootstrap

echo
echo "=== 0.1) Build updated service images (processing + aggregation) ==="
docker compose build processing aggregation

echo
echo "=== 1) Run ingestion -> processing -> aggregation ==="
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" docker compose run --rm ingestion
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" docker compose run --rm processing
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" AGG_OVERWRITE=true docker compose run --rm aggregation

echo
echo "=== 1.1) Verify curated artifact row exists ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT layer, artifact_type, schema_ver, record_id, uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'curated'
  AND artifact_type = 'window_features_v1'
  AND schema_ver = 'window_features_v1'
ORDER BY record_id;"

CURATED_URI=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'curated'
  AND artifact_type = 'window_features_v1'
  AND schema_ver = 'window_features_v1'
  AND record_id = '$RECORD_ID'
LIMIT 1;")

PROCESSED_URI=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'processed'
  AND artifact_type = 'rr_intervals_v1'
  AND schema_ver = 'rr_intervals_v1'
  AND record_id = '$RECORD_ID'
LIMIT 1;")

CURATED_URI="${CURATED_URI//[$'\r\n']}"
PROCESSED_URI="${PROCESSED_URI//[$'\r\n']}"

if [[ -z "$CURATED_URI" ]]; then
  echo "FAIL: No curated URI found for run_id=$RUN_ID record_id=$RECORD_ID"
  exit 1
fi
if [[ -z "$PROCESSED_URI" ]]; then
  echo "FAIL: No processed URI found for run_id=$RUN_ID record_id=$RECORD_ID"
  exit 1
fi

echo "PROCESSED_URI=$PROCESSED_URI"
echo "CURATED_URI=$CURATED_URI"

echo
echo "=== 2) Validate curated schema + t_peak_sec + deterministic window count ==="
docker compose run --rm \
  -e PROCESSED_URI="$PROCESSED_URI" \
  -e CURATED_URI="$CURATED_URI" \
  processing python - <<'PY'
import math
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
bucket = os.environ.get("MINIO_BUCKET", "ecg-datalake")
processed_uri = os.environ["PROCESSED_URI"]
curated_uri = os.environ["CURATED_URI"]
ak = os.environ.get("AWS_ACCESS_KEY_ID")
sk = os.environ.get("AWS_SECRET_ACCESS_KEY")

if not ak or not sk:
    raise SystemExit("FAIL: missing AWS credentials")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=ak,
    aws_secret_access_key=sk,
)

# Processed RR checks: t_peak_sec exists and has non-null values.
obj = s3.get_object(Bucket=bucket, Key=processed_uri)
rr_table = pq.read_table(pa.BufferReader(obj["Body"].read()))
if "t_peak_sec" not in rr_table.schema.names:
    raise SystemExit("FAIL: t_peak_sec column missing in processed rr_intervals_v1")
if rr_table.num_rows == 0:
    raise SystemExit("FAIL: processed rr_intervals_v1 has 0 rows")

t_col = rr_table.column("t_peak_sec")
nonnull = t_col.length() - t_col.null_count
if nonnull == 0:
    raise SystemExit("FAIL: t_peak_sec exists but all values are null")

t_vals = [v.as_py() for chunk in t_col.iterchunks() for v in chunk if v.as_py() is not None]
max_t = max(float(v) for v in t_vals)
expected_windows = int(math.floor(max_t / 300.0) + 1)

# Curated checks: prefix exists and contains parquet part files.
keys = []
token = None
while True:
    kwargs = {"Bucket": bucket, "Prefix": curated_uri}
    if token:
        kwargs["ContinuationToken"] = token
    resp = s3.list_objects_v2(**kwargs)
    keys.extend(o["Key"] for o in resp.get("Contents", []))
    if not resp.get("IsTruncated"):
        break
    token = resp.get("NextContinuationToken")

parquet_keys = [k for k in keys if k.endswith(".parquet")]
if not parquet_keys:
    raise SystemExit(f"FAIL: curated prefix has no parquet part files: {curated_uri}")

# Count curated rows and validate required curated schema columns.
actual_rows = 0
window_starts = []
required_cols = {
    "window_start_sec",
    "window_end_sec",
    "n_rr",
    "window_valid",
    "mean_rr_ms",
    "sdnn_ms",
    "rmssd_ms",
    "pnn50",
}
for key in parquet_keys:
    o = s3.get_object(Bucket=bucket, Key=key)
    t = pq.read_table(pa.BufferReader(o["Body"].read()))
    actual_rows += t.num_rows
    missing_cols = [c for c in required_cols if c not in t.schema.names]
    if missing_cols:
        raise SystemExit(f"FAIL: curated output missing required columns: {missing_cols}")
    ws_col = t.column("window_start_sec")
    window_starts.extend([v.as_py() for chunk in ws_col.iterchunks() for v in chunk if v.as_py() is not None])

if actual_rows == 0:
    raise SystemExit("FAIL: curated dataset has 0 rows")

for w in window_starts:
    if abs(float(w) % 300.0) > 1e-9:
        raise SystemExit(f"FAIL: window_start_sec not aligned to 300s: {w}")

if actual_rows != expected_windows:
    raise SystemExit(
        f"FAIL: curated window rows mismatch expected windows "
        f"(actual_rows={actual_rows}, expected={expected_windows}, max_t={max_t})"
    )

print(
    "PASS: Gate B checks passed "
    f"(rr_rows={rr_table.num_rows}, t_peak_nonnull={nonnull}, "
    f"expected_windows={expected_windows}, curated_rows={actual_rows})"
)
PY

echo
echo "=== 3) Idempotency check: second run with AGG_OVERWRITE=false should skip ==="
SECOND_LOG="$(mktemp)"
set +e
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" AGG_OVERWRITE=false docker compose run --rm aggregation | tee "$SECOND_LOG"
SECOND_EXIT="${PIPESTATUS[0]}"
set -e

if [[ "$SECOND_EXIT" -ne 0 ]]; then
  echo "FAIL: second aggregation run failed with exit code $SECOND_EXIT (expected 0)"
  exit 1
fi

if ! grep -Eq "aggregation_skip_record|All selected records skipped because curated output prefixes exist" "$SECOND_LOG"; then
  echo "FAIL: second aggregation run did not emit expected skip indicator"
  rm -f "$SECOND_LOG"
  exit 1
fi

rm -f "$SECOND_LOG"

echo
echo "Gate B contract test passed."

