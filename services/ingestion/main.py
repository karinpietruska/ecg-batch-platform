"""
Ingestion service – v0.3 multi-record + WFDB (MIT-BIH)

Supports synthetic or real WFDB data; RECORD_IDS / RECORD_RANGE; per-record
idempotency; run status succeeded / partial_success / failed; fail-per-record.
"""
import os
import sys
import json
from datetime import UTC, datetime
import tempfile

import boto3
import numpy as np
import psycopg2
import wfdb
from psycopg2.extras import RealDictCursor
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# -----------------------------------------------------------------------------
# Config from env (use service names inside Docker network)
# -----------------------------------------------------------------------------
RUN_ID = os.environ.get("RUN_ID")
RUN_DATE = os.environ.get("RUN_DATE")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecg_metadata")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "ecg")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "ecg-datalake")

# Credentials for boto3 (mapped from MINIO_ROOT_USER/PASSWORD in docker-compose)
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Record list: RECORD_IDS wins over RECORD_RANGE; if neither, default to single synthetic record
RECORD_IDS_RAW = os.environ.get("RECORD_IDS")
RECORD_RANGE_RAW = os.environ.get("RECORD_RANGE")
RECORD_LIMIT_RAW = os.environ.get("RECORD_LIMIT")  # optional, truncate for dev

# Idempotency: if true, overwrite existing object and upsert metadata; if false, skip when object exists
_OVERWRITE_RAW = (os.environ.get("INGEST_OVERWRITE", "false") or "false").strip().lower()
INGEST_OVERWRITE = _OVERWRITE_RAW in ("1", "true", "yes", "y", "on")

# Data source: synthetic (default) or WFDB from local dir
_USE_SYNTH_RAW = (os.environ.get("USE_SYNTHETIC_DATA", "true") or "true").strip().lower()
USE_SYNTHETIC_DATA = _USE_SYNTH_RAW in ("1", "true", "yes", "y", "on")
WFDB_LOCAL_DIR = (os.environ.get("WFDB_LOCAL_DIR") or "/data/mitdb").strip()
# Optional: only read first N seconds for fast dev (e.g. 10); 0 or negative = disabled
DEV_SLICE_SECONDS_RAW = os.environ.get("DEV_SLICE_SECONDS")
try:
    _dev_sec = int((DEV_SLICE_SECONDS_RAW or "").strip()) if (DEV_SLICE_SECONDS_RAW and DEV_SLICE_SECONDS_RAW.strip()) else 0
except (ValueError, AttributeError):
    _dev_sec = 0
DEV_SLICE_SECONDS = _dev_sec if _dev_sec > 0 else None


def log_structured(**kwargs):
    """Emit structured JSON plus legacy key=value logs."""
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "service": "ingestion",
        **kwargs,
    }
    print(json.dumps(payload, sort_keys=True), flush=True)
    parts = [f"{k}={v!r}" for k, v in sorted(kwargs.items())]
    print(" ".join(parts), flush=True)


def validate_run_date(s: str) -> bool:
    """Expect YYYY-MM-DD."""
    if not s or len(s) != 10:
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def resolve_record_ids() -> list[str]:
    """
    Resolve record list from RECORD_IDS or RECORD_RANGE (RECORD_IDS wins).
    If neither set, return ["synthetic_001"]. Apply RECORD_LIMIT if set.
    Raises ValueError on parse failure.
    """
    if RECORD_IDS_RAW is not None and (RECORD_IDS_RAW or "").strip():
        ids = [s.strip() for s in RECORD_IDS_RAW.split(",") if s.strip()]
        if not ids:
            raise ValueError("RECORD_IDS is empty or only separators")
        record_ids = ids
    elif RECORD_RANGE_RAW is not None and (RECORD_RANGE_RAW or "").strip():
        s = RECORD_RANGE_RAW.strip()
        if "-" not in s:
            raise ValueError("RECORD_RANGE must be start-end (e.g. 100-124)")
        parts = s.split("-", 1)
        if len(parts) != 2:
            raise ValueError("RECORD_RANGE must be start-end (e.g. 100-124)")
        try:
            start = int(parts[0].strip())
            end = int(parts[1].strip())
        except ValueError as e:
            raise ValueError(f"RECORD_RANGE start/end must be integers: {e}") from e
        if start > end:
            raise ValueError(f"RECORD_RANGE start ({start}) must be <= end ({end})")
        record_ids = [str(i) for i in range(start, end + 1)]
    else:
        record_ids = ["synthetic_001"]

    if RECORD_LIMIT_RAW is not None and RECORD_LIMIT_RAW.strip():
        try:
            limit = int(RECORD_LIMIT_RAW.strip())
        except ValueError as e:
            raise ValueError(f"RECORD_LIMIT must be an integer: {e}") from e
        if limit < 1:
            raise ValueError("RECORD_LIMIT must be >= 1")
        record_ids = record_ids[:limit]

    return record_ids


def get_s3_client():
    """S3 client for MinIO (use host minio in Docker network)."""
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set for MinIO access")
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def object_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if object exists; False if 404/NoSuchKey/NotFound; re-raise other errors."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = str((e.response.get("Error") or {}).get("Code", "")).strip()
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def generate_synthetic_ecg(run_id: str, run_date: str, record_id: str):
    """Generate 60s of synthetic ECG-like data at 360 Hz for two leads."""
    fs = 360
    duration_sec = 60
    n_samples = fs * duration_sec

    sample_index = np.arange(n_samples, dtype=np.int64)
    t_sec = sample_index / float(fs)

    # Simple synthetic signals (sine waves with phase shift)
    lead_0 = np.sin(2 * np.pi * 1.0 * t_sec)
    lead_1 = np.sin(2 * np.pi * 1.0 * t_sec + np.pi / 4.0)

    run_id_vals = np.full(n_samples, run_id, dtype=object)
    run_date_vals = np.full(n_samples, run_date, dtype=object)
    record_id_vals = np.full(n_samples, record_id, dtype=object)

    table = pa.table(
        {
            "run_id": pa.array(run_id_vals),
            "run_date": pa.array(run_date_vals),
            "record_id": pa.array(record_id_vals),
            "sample_index": pa.array(sample_index),
            "t_sec": pa.array(t_sec),
            "lead_0": pa.array(lead_0),
            "lead_1": pa.array(lead_1),
        }
    )

    return table, fs, n_samples


def load_wfdb_record(run_id: str, run_date: str, record_id: str):
    """
    Load one WFDB record from WFDB_LOCAL_DIR and build Parquet-ready table.
    Uses p_signal if present, else d_signal.astype(np.float32).
    Single channel: lead_1 set to zeros and log. Optional DEV_SLICE_SECONDS truncates.
    Returns (table, fs, n_samples, n_channels, atr_exists).
    Raises on missing/invalid record so caller can count records_failed.
    """
    path = os.path.join(WFDB_LOCAL_DIR, record_id)
    record = wfdb.rdrecord(path)

    if record.p_signal is not None:
        sig = np.asarray(record.p_signal, dtype=np.float32)
    else:
        sig = np.asarray(record.d_signal, dtype=np.float32)

    fs = int(record.fs)
    n_total = sig.shape[0]
    n_ch = sig.shape[1] if sig.ndim > 1 else 1
    if sig.ndim == 1:
        sig = sig.reshape(-1, 1)

    if DEV_SLICE_SECONDS is not None and DEV_SLICE_SECONDS > 0:
        max_samples = int(fs * DEV_SLICE_SECONDS)
        sig = sig[:max_samples]

    n_samples = sig.shape[0]  # always from sliced array

    sample_index = np.arange(n_samples, dtype=np.int64)
    t_sec = sample_index / float(fs)

    lead_0 = sig[:, 0]
    if n_ch >= 2:
        lead_1 = sig[:, 1]
        n_channels = 2
    else:
        lead_1 = np.zeros(n_samples, dtype=np.float32)
        n_channels = 1
        log_structured(event="wfdb_single_channel", record_id=record_id)

    run_id_vals = np.full(n_samples, run_id, dtype=object)
    run_date_vals = np.full(n_samples, run_date, dtype=object)
    record_id_vals = np.full(n_samples, record_id, dtype=object)

    table = pa.table(
        {
            "run_id": pa.array(run_id_vals),
            "run_date": pa.array(run_date_vals),
            "record_id": pa.array(record_id_vals),
            "sample_index": pa.array(sample_index),
            "t_sec": pa.array(t_sec),
            "lead_0": pa.array(lead_0),
            "lead_1": pa.array(lead_1),
        }
    )

    atr_path = os.path.join(WFDB_LOCAL_DIR, f"{record_id}.atr")
    atr_exists = os.path.exists(atr_path)

    return table, fs, n_samples, n_channels, atr_exists


def upload_parquet_to_minio(table: pa.Table, bucket: str, key: str, s3_client=None) -> None:
    """Write table to a temp Parquet file and upload to MinIO via boto3."""
    if s3_client is None:
        s3_client = get_s3_client()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        pq.write_table(table, tmp_path)
        s3_client.upload_file(tmp_path, bucket, key)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def main() -> int:
    run_id = (RUN_ID or "").strip()
    run_date = (RUN_DATE or "").strip()

    if not run_id:
        log_structured(event="ingestion_start", error="RUN_ID missing or empty")
        return 1
    if not run_date or not validate_run_date(run_date):
        log_structured(
            event="ingestion_start",
            error="RUN_DATE missing or invalid (use YYYY-MM-DD)",
            run_id=run_id,
        )
        return 1

    log_structured(
        event="ingestion_start",
        run_id=run_id,
        run_date=run_date,
        use_synthetic=USE_SYNTHETIC_DATA,
        wfdb_dir=WFDB_LOCAL_DIR if not USE_SYNTHETIC_DATA else None,
    )

    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            cursor_factory=RealDictCursor,
        )
        conn.autocommit = False
        cur = conn.cursor()

        # Upsert run with status=running (for reruns we overwrite)
        cur.execute(
            """
            INSERT INTO runs (run_id, run_date, status, notes)
            VALUES (%s, %s, 'running', 'ingestion v0.3 multi-record')
            ON CONFLICT (run_id) DO UPDATE SET
                run_date = EXCLUDED.run_date,
                status   = 'running',
                notes    = EXCLUDED.notes
            """,
            (run_id, run_date),
        )
        conn.commit()

        # Resolve record list (after run exists so we can mark run failed on parse error)
        try:
            record_ids = resolve_record_ids()
        except ValueError as e:
            cur.execute(
                "UPDATE runs SET status = 'failed', notes = %s WHERE run_id = %s",
                (str(e)[:500], run_id),
            )
            conn.commit()
            log_structured(
                event="record_list_parse_failed",
                run_id=run_id,
                detail=str(e),
            )
            return 1

        if not record_ids:
            cur.execute(
                "UPDATE runs SET status = 'failed', notes = %s WHERE run_id = %s",
                ("record list empty", run_id),
            )
            conn.commit()
            log_structured(
                event="record_list_empty",
                run_id=run_id,
            )
            return 1

        s3_client = get_s3_client()
        records_ok = 0
        records_skipped = 0
        records_failed = 0

        for record_id in record_ids:
            object_key = (
                f"raw/run_date={run_date}/run_id={run_id}/"
                f"record_id={record_id}/ecg.parquet"
            )

            # Idempotency: check before generating data
            if object_exists(s3_client, MINIO_BUCKET, object_key):
                if not INGEST_OVERWRITE:
                    log_structured(
                        event="ingestion_skip",
                        record_id=record_id,
                        reason="object_exists",
                    )
                    records_skipped += 1
                    continue
                # overwrite=true: fall through to write

            try:
                if USE_SYNTHETIC_DATA:
                    table, fs, n_samples = generate_synthetic_ecg(run_id, run_date, record_id)
                    n_channels = 2
                    atr_exists = False
                else:
                    table, fs, n_samples, n_channels, atr_exists = load_wfdb_record(
                        run_id, run_date, record_id
                    )
                upload_parquet_to_minio(table, MINIO_BUCKET, object_key, s3_client=s3_client)
                cur.execute(
                    """
                    INSERT INTO artifacts (run_id, record_id, layer, artifact_type, uri, schema_ver)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, record_id, layer, artifact_type)
                    DO UPDATE SET uri = EXCLUDED.uri, schema_ver = EXCLUDED.schema_ver, created_at = NOW()
                    """,
                    (run_id, record_id, "raw", "ecg", object_key, "raw_ecg_v1"),
                )
                cur.execute(
                    """
                    INSERT INTO quality_metrics (run_id, record_id, sampling_hz, n_samples, n_channels, atr_exists)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, record_id)
                    DO UPDATE SET
                        sampling_hz = EXCLUDED.sampling_hz,
                        n_samples = EXCLUDED.n_samples,
                        n_channels = EXCLUDED.n_channels,
                        atr_exists = EXCLUDED.atr_exists,
                        created_at = NOW()
                    """,
                    (run_id, record_id, fs, n_samples, n_channels, atr_exists),
                )
                records_ok += 1
            except Exception as e:
                log_structured(
                    event="ingestion_record_failed",
                    record_id=record_id,
                    error=str(e),
                )
                records_failed += 1
                continue

        conn.commit()

        # Run status from counts (skipped counts as success: failed==0 → succeeded)
        if records_failed == 0:
            status = "succeeded"
        elif records_ok > 0 or records_skipped > 0:
            status = "partial_success"
        else:
            status = "failed"

        cur.execute(
            "UPDATE runs SET status = %s WHERE run_id = %s",
            (status, run_id),
        )
        conn.commit()

        log_structured(
            event="ingestion_end",
            run_id=run_id,
            status=status,
            records_total=len(record_ids),
            records_ok=records_ok,
            records_skipped=records_skipped,
            records_failed=records_failed,
            overwrite="true" if INGEST_OVERWRITE else "false",
        )
        return 0 if records_failed == 0 else 1

    except Exception as e:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE runs SET status = 'failed', notes = %s WHERE run_id = %s",
                    (str(e)[:500], run_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                conn.close()
        log_structured(event="ingestion_end", run_id=run_id, status="failed", error=str(e))
        return 1
    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
