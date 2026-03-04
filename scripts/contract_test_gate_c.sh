#!/usr/bin/env bash
# Gate C contract test:
# - Produces processed -> curated -> ml_ready for one record.
# - Verifies canonical ml_ready artifact metadata.
# - Verifies record_features_v1 schema + semantic checks.
# - Verifies n_rr invariant: sum(window_features_v1.n_rr) == ml_ready.n_rr == processed rr row count.
# - Verifies Gate C idempotency: second run with AGG_OVERWRITE=false skips on ml_ready prefix.

set -euo pipefail

cd "$(dirname "$0")/.."

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ID="${RUN_ID:-${RUN_DATE}_gateC_${STAMP}}"
RECORD_ID="${RECORD_ID:-100}"

echo "=== Gate C contract test ==="
echo "RUN_DATE=$RUN_DATE"
echo "RUN_ID=$RUN_ID"
echo "RECORD_ID=$RECORD_ID"

echo
echo "=== 0) Ensure infra ==="
docker compose up -d postgres minio minio-bootstrap

echo
echo "=== 0.1) Build updated images ==="
docker compose build ingestion processing aggregation

echo
echo "=== 1) Run ingestion -> processing -> aggregation ==="
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" docker compose run --rm ingestion
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" docker compose run --rm processing
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" AGG_OVERWRITE=true docker compose run --rm aggregation

echo
echo "=== 1.1) Verify canonical ml_ready artifact row exists ==="
docker compose exec postgres psql -U ecg -d ecg_metadata -c "
SELECT layer, artifact_type, schema_ver, record_id, uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'ml_ready'
  AND artifact_type = 'record_features_v1'
  AND schema_ver = 'record_features_v1'
ORDER BY record_id;"

ML_READY_URI=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT uri
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'ml_ready'
  AND artifact_type = 'record_features_v1'
  AND schema_ver = 'record_features_v1'
  AND record_id = '$RECORD_ID'
LIMIT 1;")

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

ML_READY_URI="${ML_READY_URI//[$'\r\n']}"
CURATED_URI="${CURATED_URI//[$'\r\n']}"
PROCESSED_URI="${PROCESSED_URI//[$'\r\n']}"

if [[ -z "$ML_READY_URI" || -z "$CURATED_URI" || -z "$PROCESSED_URI" ]]; then
  echo "FAIL: missing one or more canonical URIs"
  echo "ML_READY_URI=$ML_READY_URI"
  echo "CURATED_URI=$CURATED_URI"
  echo "PROCESSED_URI=$PROCESSED_URI"
  exit 1
fi

echo "ML_READY_URI=$ML_READY_URI"
echo "CURATED_URI=$CURATED_URI"
echo "PROCESSED_URI=$PROCESSED_URI"

echo
echo "=== 2) Validate record_features_v1 schema + semantics + invariants ==="
docker compose run --rm \
  -e ML_READY_URI="$ML_READY_URI" \
  -e CURATED_URI="$CURATED_URI" \
  -e PROCESSED_URI="$PROCESSED_URI" \
  -e RUN_ID="$RUN_ID" \
  -e RECORD_ID="$RECORD_ID" \
  processing python - <<'PY'
import math
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
bucket = os.environ.get("MINIO_BUCKET", "ecg-datalake")
ml_ready_uri = os.environ["ML_READY_URI"]
curated_uri = os.environ["CURATED_URI"]
processed_uri = os.environ["PROCESSED_URI"]
run_id = os.environ["RUN_ID"]
record_id = os.environ["RECORD_ID"]
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

def list_parquet(prefix: str):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in resp.get("Contents", []))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return [k for k in keys if k.endswith(".parquet")]

required_cols = {
    "run_id", "run_date", "record_id",
    "mean_rr_ms", "sdnn_ms", "rmssd_ms", "pnn50", "n_rr",
    "heart_rate_bpm", "rr_cv", "rmssd_sdnn_ratio",
    "rr_min_ms", "rr_max_ms", "rr_range_ms", "rr_median_ms", "rr_iqr_ms",
    "sdsd_ms", "pnn20",
    "mean_rr_window_std", "sdnn_window_std", "rmssd_window_std",
    "window_count_total", "window_count_valid", "valid_window_fraction",
    "mean_window_coverage_sec", "min_window_coverage_sec", "partial_window_fraction",
    "created_at",
}

ml_keys = list_parquet(ml_ready_uri)
if not ml_keys:
    raise SystemExit(f"FAIL: no ml_ready parquet files under {ml_ready_uri}")

rows = []
for key in ml_keys:
    obj = s3.get_object(Bucket=bucket, Key=key)
    t = pq.read_table(pa.BufferReader(obj["Body"].read()))
    missing = sorted([c for c in required_cols if c not in t.schema.names])
    if missing:
        raise SystemExit(f"FAIL: ml_ready missing required columns: {missing}")
    rows.extend(t.to_pylist())

target = [r for r in rows if str(r["run_id"]) == run_id and str(r["record_id"]) == record_id]
if len(target) != 1:
    raise SystemExit(f"FAIL: expected exactly one ml_ready row for run_id/record_id, got {len(target)}")
r = target[0]

def check_fraction(name):
    v = r.get(name)
    if v is None:
        return
    vf = float(v)
    if vf < 0.0 or vf > 1.0:
        raise SystemExit(f"FAIL: {name} out of [0,1]: {vf}")

check_fraction("pnn50")
check_fraction("pnn20")
check_fraction("valid_window_fraction")
check_fraction("partial_window_fraction")

rr_min = r.get("rr_min_ms")
rr_max = r.get("rr_max_ms")
rr_med = r.get("rr_median_ms")
rr_rng = r.get("rr_range_ms")
if rr_min is not None and rr_max is not None and rr_rng is not None:
    if float(rr_rng) < 0:
        raise SystemExit(f"FAIL: rr_range_ms negative: {rr_rng}")
    if abs(float(rr_rng) - (float(rr_max) - float(rr_min))) > 1e-6:
        raise SystemExit("FAIL: rr_range_ms != rr_max_ms - rr_min_ms")
if rr_min is not None and rr_med is not None and rr_max is not None:
    if not (float(rr_min) <= float(rr_med) <= float(rr_max)):
        raise SystemExit("FAIL: rr_median_ms not between rr_min_ms and rr_max_ms")

min_cov = r.get("min_window_coverage_sec")
mean_cov = r.get("mean_window_coverage_sec")
if min_cov is not None and mean_cov is not None:
    if not (0.0 <= float(min_cov) <= float(mean_cov) <= 300.0 + 1e-9):
        raise SystemExit("FAIL: coverage rollup bounds violated")

# Derived metric guard checks (prevent NaN/Inf drift).
mean_rr = r.get("mean_rr_ms")
sdnn = r.get("sdnn_ms")
hr = r.get("heart_rate_bpm")
rr_cv = r.get("rr_cv")
ratio = r.get("rmssd_sdnn_ratio")

if mean_rr is not None and float(mean_rr) <= 0:
    if hr is not None:
        raise SystemExit("FAIL: heart_rate_bpm must be NULL when mean_rr_ms <= 0")
    if rr_cv is not None:
        raise SystemExit("FAIL: rr_cv must be NULL when mean_rr_ms <= 0")
if mean_rr is not None and float(mean_rr) > 0:
    if hr is None or (not math.isfinite(float(hr))) or float(hr) <= 0:
        raise SystemExit("FAIL: heart_rate_bpm must be finite and > 0 when mean_rr_ms > 0")
    if rr_cv is not None and not math.isfinite(float(rr_cv)):
        raise SystemExit("FAIL: rr_cv must be finite when mean_rr_ms > 0")

if sdnn is not None and float(sdnn) <= 0:
    if ratio is not None:
        raise SystemExit("FAIL: rmssd_sdnn_ratio must be NULL when sdnn_ms <= 0")
if sdnn is not None and float(sdnn) > 0:
    if ratio is not None and not math.isfinite(float(ratio)):
        raise SystemExit("FAIL: rmssd_sdnn_ratio must be finite when sdnn_ms > 0")

# Invariant checks:
# 1) sum(window n_rr) == ml_ready n_rr
# 2) processed rr row count (non-null rr_interval_sec) == ml_ready n_rr
curated_keys = list_parquet(curated_uri)
if not curated_keys:
    raise SystemExit(f"FAIL: no curated parquet files under {curated_uri}")
sum_window_n_rr = 0
for key in curated_keys:
    obj = s3.get_object(Bucket=bucket, Key=key)
    t = pq.read_table(pa.BufferReader(obj["Body"].read()))
    if "n_rr" not in t.schema.names:
        raise SystemExit("FAIL: curated output missing n_rr")
    col = t.column("n_rr")
    for chunk in col.iterchunks():
        for v in chunk:
            vv = v.as_py()
            if vv is not None:
                sum_window_n_rr += int(vv)

obj = s3.get_object(Bucket=bucket, Key=processed_uri)
rr_table = pq.read_table(pa.BufferReader(obj["Body"].read()))
if "rr_interval_sec" not in rr_table.schema.names:
    raise SystemExit("FAIL: processed rr_intervals_v1 missing rr_interval_sec")
rr_col = rr_table.column("rr_interval_sec")
processed_n_rr = rr_col.length() - rr_col.null_count

ml_n_rr = r.get("n_rr")
if ml_n_rr is None:
    raise SystemExit("FAIL: ml_ready n_rr is NULL")
ml_n_rr = int(ml_n_rr)

if sum_window_n_rr != ml_n_rr:
    raise SystemExit(
        f"FAIL: invariant mismatch sum(window n_rr) != ml_ready n_rr "
        f"({sum_window_n_rr} != {ml_n_rr})"
    )
if processed_n_rr != ml_n_rr:
    raise SystemExit(
        f"FAIL: invariant mismatch processed rr count != ml_ready n_rr "
        f"({processed_n_rr} != {ml_n_rr})"
    )

print(
    "PASS: Gate C schema/semantic checks passed "
    f"(ml_ready_rows={len(target)}, ml_n_rr={ml_n_rr}, sum_window_n_rr={sum_window_n_rr})"
)
PY

echo
echo "=== 3) Idempotency check: second run with AGG_OVERWRITE=false skips on ml_ready ==="
ML_READY_COUNT_BEFORE=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT COUNT(*)
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'ml_ready'
  AND artifact_type = 'record_features_v1'
  AND schema_ver = 'record_features_v1';")
ML_READY_COUNT_BEFORE="${ML_READY_COUNT_BEFORE//[$'\r\n ']}"

SECOND_LOG="$(mktemp)"
set +e
RUN_ID="$RUN_ID" RUN_DATE="$RUN_DATE" RECORD_IDS="$RECORD_ID" AGG_OVERWRITE=false docker compose run --rm aggregation | tee "$SECOND_LOG"
SECOND_EXIT="${PIPESTATUS[0]}"
set -e

if [[ "$SECOND_EXIT" -ne 0 ]]; then
  echo "FAIL: second aggregation run failed with exit code $SECOND_EXIT (expected 0)"
  exit 1
fi
if ! grep -Eq "target='ml_ready'|Skipped because ml_ready output prefix exists" "$SECOND_LOG"; then
  echo "FAIL: second run did not emit expected ml_ready skip indicator"
  rm -f "$SECOND_LOG"
  exit 1
fi

ML_READY_COUNT_AFTER=$(docker compose exec -T postgres psql -U ecg -d ecg_metadata -t -A -c "
SELECT COUNT(*)
FROM artifacts
WHERE run_id = '$RUN_ID'
  AND layer = 'ml_ready'
  AND artifact_type = 'record_features_v1'
  AND schema_ver = 'record_features_v1';")
ML_READY_COUNT_AFTER="${ML_READY_COUNT_AFTER//[$'\r\n ']}"
if [[ "$ML_READY_COUNT_AFTER" != "$ML_READY_COUNT_BEFORE" ]]; then
  echo "FAIL: ml_ready artifact count changed on skip run (before=$ML_READY_COUNT_BEFORE after=$ML_READY_COUNT_AFTER)"
  rm -f "$SECOND_LOG"
  exit 1
fi

rm -f "$SECOND_LOG"

echo
echo "Gate C contract test passed."

