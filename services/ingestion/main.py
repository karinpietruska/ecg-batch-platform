"""
Ingestion service – Path B (v0.2)

Extends run registration by generating synthetic ECG data, writing one Parquet
file to the raw layer in MinIO, and inserting one artifact + one QC row.
"""
import os
import sys
from datetime import datetime
import tempfile

import boto3
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
import pyarrow as pa
import pyarrow.parquet as pq

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

RECORD_ID = os.environ.get("RECORD_ID", "synthetic_001")


def log_structured(**kwargs):
    """Minimal structured log (key=value)."""
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


def upload_parquet_to_minio(table: pa.Table, bucket: str, key: str) -> None:
    """Write table to a temp Parquet file and upload to MinIO via boto3."""
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set for MinIO access")

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

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

    log_structured(event="ingestion_start", run_id=run_id, run_date=run_date)

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
            VALUES (%s, %s, 'running', 'ingestion v0.2 synthetic Path B')
            ON CONFLICT (run_id) DO UPDATE SET
                run_date = EXCLUDED.run_date,
                status   = 'running',
                notes    = EXCLUDED.notes
            """,
            (run_id, run_date),
        )
        conn.commit()

        # Generate synthetic data and write one Parquet file to MinIO (raw layer)
        table, fs, n_samples = generate_synthetic_ecg(run_id, run_date, RECORD_ID)

        object_key = (
            f"raw/run_date={run_date}/run_id={run_id}/"
            f"record_id={RECORD_ID}/ecg.parquet"
        )

        upload_parquet_to_minio(table, MINIO_BUCKET, object_key)

        # Insert artifact + quality metrics
        cur.execute(
            """
            INSERT INTO artifacts (run_id, record_id, layer, artifact_type, uri, schema_ver)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, RECORD_ID, "raw", "ecg", object_key, "raw_ecg_v1"),
        )

        cur.execute(
            """
            INSERT INTO quality_metrics (run_id, record_id, sampling_hz, n_samples, n_channels, atr_exists)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, RECORD_ID, fs, n_samples, 2, False),
        )

        conn.commit()

        # Mark run as succeeded
        cur.execute(
            "UPDATE runs SET status = 'succeeded' WHERE run_id = %s",
            (run_id,),
        )
        conn.commit()

        log_structured(event="ingestion_end", run_id=run_id, status="succeeded")
        return 0

    except Exception as e:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE runs SET status = 'failed', notes = %s WHERE run_id = %s",
                    (str(e)[:500], (RUN_ID or "").strip()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                conn.close()
        log_structured(event="ingestion_end", run_id=RUN_ID, status="failed", error=str(e))
        return 1
    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
