"""
Processing service – skeleton v0.1

Reads raw ECG artifacts for a run, and writes empty-but-typed rr_intervals_v1
artifacts plus metadata. No R-peak detection yet.
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
from botocore.exceptions import ClientError


RUN_ID = os.environ.get("RUN_ID")
RUN_DATE = os.environ.get("RUN_DATE")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecg_metadata")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "ecg")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "ecg-datalake")

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

RECORD_IDS_RAW = os.environ.get("RECORD_IDS")
RECORD_RANGE_RAW = os.environ.get("RECORD_RANGE")
RECORD_LIMIT_RAW = os.environ.get("RECORD_LIMIT")

_OVERWRITE_RAW = (os.environ.get("PROCESS_OVERWRITE", "false") or "false").strip().lower()
PROCESS_OVERWRITE = _OVERWRITE_RAW in ("1", "true", "yes", "y", "on")


def log_structured(**kwargs):
    parts = [f"{k}={v!r}" for k, v in sorted(kwargs.items())]
    print(" ".join(parts), flush=True)


def validate_run_date(s: str) -> bool:
    if not s or len(s) != 10:
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_s3_client():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set for MinIO access")
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = str((e.response.get("Error") or {}).get("Code", "")).strip()
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def download_parquet_from_minio(bucket: str, key: str, s3_client=None) -> pa.Table:
    if s3_client is None:
        s3_client = get_s3_client()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        s3_client.download_file(bucket, key, tmp_path)
        return pq.read_table(tmp_path)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def upload_parquet_to_minio(table: pa.Table, bucket: str, key: str, s3_client=None) -> None:
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


def resolve_record_filters() -> set[str] | None:
    """
    Resolve optional record filters from RECORD_IDS / RECORD_RANGE / RECORD_LIMIT.
    Returns:
      - None if no filters were provided.
      - A set of record_id strings if filters are present.
    Raises ValueError on invalid input.
    """
    have_ids = RECORD_IDS_RAW is not None and (RECORD_IDS_RAW or "").strip()
    have_range = RECORD_RANGE_RAW is not None and (RECORD_RANGE_RAW or "").strip()

    if not have_ids and not have_range and not (RECORD_LIMIT_RAW and RECORD_LIMIT_RAW.strip()):
        return None

    record_ids: list[str]
    if have_ids:
        ids = [s.strip() for s in RECORD_IDS_RAW.split(",") if s.strip()]
        if not ids:
            raise ValueError("RECORD_IDS is empty or only separators")
        record_ids = ids
    elif have_range:
        s = RECORD_RANGE_RAW.strip()
        if "-" not in s:
            raise ValueError("RECORD_RANGE must be start-end (e.g. 100-124)")
        start_s, end_s = s.split("-", 1)
        try:
            start = int(start_s.strip())
            end = int(end_s.strip())
        except ValueError as e:
            raise ValueError(f"RECORD_RANGE start/end must be integers: {e}") from e
        if start > end:
            raise ValueError(f"RECORD_RANGE start ({start}) must be <= end ({end})")
        record_ids = [str(i) for i in range(start, end + 1)]
    else:
        record_ids = []

    if RECORD_LIMIT_RAW and RECORD_LIMIT_RAW.strip():
        try:
            limit = int(RECORD_LIMIT_RAW.strip())
        except ValueError as e:
            raise ValueError(f"RECORD_LIMIT must be an integer: {e}") from e
        if limit < 1:
            raise ValueError("RECORD_LIMIT must be >= 1")
        record_ids = record_ids[:limit]

    return set(record_ids)


def discover_raw_records(conn, run_id: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT record_id
        FROM artifacts
        WHERE run_id = %s AND layer = 'raw' AND artifact_type = 'ecg'
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    return {row["record_id"] for row in rows}


def empty_rr_intervals_table(run_id: str, run_date: str, record_id: str) -> pa.Table:
    """
    Build an empty rr_intervals_v1 table with the final schema and zero rows.
    Columns:
      - run_id (string)
      - run_date (string)
      - record_id (string)
      - beat_index (int64)
      - peak_index (int64)
      - t_peak_sec (float64)
      - rr_interval_sec (float64, nullable)
    """
    return pa.table(
        {
            "run_id": pa.array([], type=pa.string()),
            "run_date": pa.array([], type=pa.string()),
            "record_id": pa.array([], type=pa.string()),
            "beat_index": pa.array([], type=pa.int64()),
            "peak_index": pa.array([], type=pa.int64()),
            "t_peak_sec": pa.array([], type=pa.float64()),
            "rr_interval_sec": pa.array([], type=pa.float64()),
        }
    )


def upsert_service_run(cur, run_id: str, status: str, notes: str | None, set_started: bool, set_ended: bool):
    fields = ["status = EXCLUDED.status", "notes = EXCLUDED.notes"]
    timestamps = []
    if set_started:
        timestamps.append("started_at = NOW()")
    if set_ended:
        timestamps.append("ended_at = NOW()")
    if timestamps:
        fields.extend(timestamps)

    cur.execute(
        f"""
        INSERT INTO service_runs (run_id, service, status, notes, started_at, ended_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, service)
        DO UPDATE SET {", ".join(fields)}
        """,
        (run_id, "processing", status, notes, datetime.utcnow(), None),
    )


def main() -> int:
    run_id = (RUN_ID or "").strip()
    run_date = (RUN_DATE or "").strip()

    if not run_id:
        log_structured(event="processing_start", error="RUN_ID missing or empty")
        return 1
    if not run_date or not validate_run_date(run_date):
        log_structured(
            event="processing_start",
            error="RUN_DATE missing or invalid (use YYYY-MM-DD)",
            run_id=run_id,
        )
        return 1

    log_structured(
        event="processing_start",
        run_id=run_id,
        run_date=run_date,
        overwrite=PROCESS_OVERWRITE,
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

        # service_runs: mark processing as running
        upsert_service_run(cur, run_id, status="running", notes="processing skeleton v0.1", set_started=True, set_ended=False)
        conn.commit()

        # Discover available raw records
        raw_records = discover_raw_records(conn, run_id)

        # Resolve optional filters
        try:
            filter_ids = resolve_record_filters()
        except ValueError as e:
            # Validation failure: mark service run failed and exit 1
            upsert_service_run(cur, run_id, status="failed", notes=str(e)[:500], set_started=False, set_ended=True)
            conn.commit()
            log_structured(
                event="processing_record_filter_invalid",
                run_id=run_id,
                detail=str(e),
            )
            return 1

        if filter_ids is None:
            # No filters provided: process all discovered raw records
            record_ids = sorted(raw_records)
            if not record_ids:
                # Nothing to do is a clean success
                upsert_service_run(
                    cur,
                    run_id,
                    status="succeeded",
                    notes="no raw artifacts found for processing",
                    set_started=False,
                    set_ended=True,
                )
                conn.commit()
                log_structured(
                    event="processing_no_raw_records",
                    run_id=run_id,
                )
                return 0
        else:
            # Filters provided: intersect with discovered raw records
            record_ids = sorted(raw_records.intersection(filter_ids))
            if not record_ids:
                upsert_service_run(
                    cur,
                    run_id,
                    status="failed",
                    notes="record filter did not match any raw artifacts",
                    set_started=False,
                    set_ended=True,
                )
                conn.commit()
                log_structured(
                    event="processing_no_records_after_filter",
                    run_id=run_id,
                )
                return 1

        s3_client = get_s3_client()
        records_ok = 0
        records_skipped = 0
        records_failed = 0

        for record_id in record_ids:
            raw_key = f"raw/run_date={run_date}/run_id={run_id}/record_id={record_id}/ecg.parquet"
            proc_key = (
                f"processing/run_date={run_date}/run_id={run_id}/"
                f"record_id={record_id}/rr_intervals.parquet"
            )

            # Idempotency: skip if processing artifact exists and overwrite is false
            if object_exists(s3_client, MINIO_BUCKET, proc_key):
                if not PROCESS_OVERWRITE:
                    log_structured(
                        event="processing_skip",
                        record_id=record_id,
                        reason="object_exists",
                    )
                    records_skipped += 1
                    continue

            try:
                # Ensure raw exists (will raise if missing)
                if not object_exists(s3_client, MINIO_BUCKET, raw_key):
                    raise FileNotFoundError(f"raw artifact missing at {raw_key}")
                # For now we don't need the content, but this validates readability
                _ = download_parquet_from_minio(MINIO_BUCKET, raw_key, s3_client=s3_client)

                table = empty_rr_intervals_table(run_id, run_date, record_id)
                upload_parquet_to_minio(table, MINIO_BUCKET, proc_key, s3_client=s3_client)

                # Upsert artifact metadata
                cur.execute(
                    """
                    INSERT INTO artifacts (run_id, record_id, layer, artifact_type, uri, schema_ver)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, record_id, layer, artifact_type)
                    DO UPDATE SET uri = EXCLUDED.uri, schema_ver = EXCLUDED.schema_ver, created_at = NOW()
                    """,
                    (run_id, record_id, "processing", "rr_intervals", proc_key, "rr_intervals_v1"),
                )
                # Upsert dummy processing_metrics row
                cur.execute(
                    """
                    INSERT INTO processing_metrics (run_id, record_id, n_beats, mean_rr_ms, sdnn_ms)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, record_id)
                    DO UPDATE SET
                        n_beats = EXCLUDED.n_beats,
                        mean_rr_ms = EXCLUDED.mean_rr_ms,
                        sdnn_ms = EXCLUDED.sdnn_ms,
                        created_at = NOW()
                    """,
                    (run_id, record_id, None, None, None),
                )

                records_ok += 1
            except Exception as e:
                log_structured(
                    event="processing_record_failed",
                    record_id=record_id,
                    error=str(e),
                )
                records_failed += 1
                continue

        conn.commit()

        if records_failed == 0:
            status = "succeeded"
        elif records_ok > 0 or records_skipped > 0:
            status = "partial_success"
        else:
            status = "failed"

        notes = (
            f"records_total={len(record_ids)},"
            f"records_ok={records_ok},"
            f"records_skipped={records_skipped},"
            f"records_failed={records_failed}"
        )

        upsert_service_run(
            cur,
            run_id,
            status=status,
            notes=notes,
            set_started=False,
            set_ended=True,
        )
        conn.commit()

        log_structured(
            event="processing_end",
            run_id=run_id,
            status=status,
            records_total=len(record_ids),
            records_ok=records_ok,
            records_skipped=records_skipped,
            records_failed=records_failed,
            overwrite="true" if PROCESS_OVERWRITE else "false",
        )

        return 0 if records_failed == 0 else 1

    except Exception as e:
        if conn:
            try:
                cur = conn.cursor()
                upsert_service_run(
                    cur,
                    run_id,
                    status="failed",
                    notes=str(e)[:500],
                    set_started=False,
                    set_ended=True,
                )
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                conn.close()
        log_structured(event="processing_end", run_id=run_id, status="failed", error=str(e))
        return 1
    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())

