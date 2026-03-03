"""
Aggregation service – curated 5-minute HRV windows (Spark + S3A)

Discovers canonical processed rr_intervals_v1 artifacts for a run, reads them via Spark from MinIO (S3A),
computes per-record 5-minute window HRV metrics, writes curated window_features_v1 datasets, and upserts
curated artifacts.
"""
import os
import sys
from datetime import UTC, datetime

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession


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

_OVERWRITE_RAW = (os.environ.get("AGG_OVERWRITE", "false") or "false").strip().lower()
AGG_OVERWRITE = _OVERWRITE_RAW in ("1", "true", "yes", "y", "on")

def curated_output_key(run_date: str, run_id: str, record_id: str) -> str:
    """Per-record curated output prefix."""
    return f"curated/run_date={run_date}/run_id={run_id}/record_id={record_id}/window_features_v1.parquet/"


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


def prefix_exists(s3_client, bucket: str, prefix: str) -> bool:
    """Return True if any object exists with the given prefix (e.g. Spark writes a directory with part files).
    Prefix-based: treats any object under the path as 'exists'. For strict 'complete output' you could
    check for prefix/_SUCCESS; current behavior can treat partial/incomplete writes as exists."""
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except ClientError:
        return False


def resolve_record_filters() -> set[str] | None:
    """Same semantics as processing: None = no filters, set = filter set. Raises ValueError on invalid input."""
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


def discover_processing_artifacts(conn, run_id: str) -> list[dict]:
    """Return canonical RR artifacts for aggregation input (processed/rr_intervals_v1)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT record_id, uri
        FROM artifacts
        WHERE run_id = %s
          AND layer = 'processed'
          AND artifact_type = 'rr_intervals_v1'
          AND schema_ver = 'rr_intervals_v1'
        ORDER BY record_id
        """,
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def build_spark_session() -> SparkSession:
    """Spark with S3A support for MinIO (no SSL)."""
    # S3A/Hadoop expect numeric strings for timeouts and pool settings; Spark may pass
    # duration strings (e.g. "60s") which cause NumberFormatException in getLong().
    # Override with plain numbers so S3A init succeeds.
    builder = (
        SparkSession.builder.appName("ecg-aggregation")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID or "")
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY or "")
        # Use v1 credential provider (SimpleAWSCredentialsProvider reads access/secret from config).
        # Default can point at AWS SDK v2 classes which are not on classpath (aws-java-sdk-bundle is v1).
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        # Multipart upload purge/expiration: S3A expects numeric; Spark may pass "24h".
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")  # 24h in seconds
    )
    return builder.getOrCreate()


def s3a_path(bucket: str, key: str) -> str:
    """Build S3A path. DB artifact.uri is object key only (no scheme); processing/ingestion store keys like processing/run_date=.../..."""
    return f"s3a://{bucket}/{key}"


def upsert_service_run(cur, run_id: str, status: str, notes: str | None, set_started: bool, set_ended: bool):
    fields = ["status = EXCLUDED.status", "notes = EXCLUDED.notes"]
    timestamps = []
    if set_started:
        timestamps.append("started_at = EXCLUDED.started_at")
    if set_ended:
        timestamps.append("ended_at = COALESCE(service_runs.ended_at, EXCLUDED.ended_at)")
    if timestamps:
        fields.extend(timestamps)
    started_ts = datetime.now(UTC) if set_started else None
    ended_ts = datetime.now(UTC) if set_ended else None
    cur.execute(
        f"""
        INSERT INTO service_runs (run_id, service, status, notes, started_at, ended_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, service)
        DO UPDATE SET {", ".join(fields)}
        """,
        (run_id, "aggregation", status, notes, started_ts, ended_ts),
    )


def main() -> int:
    run_id = (RUN_ID or "").strip()
    run_date = (RUN_DATE or "").strip()

    if not run_id:
        log_structured(event="aggregation_start", error="RUN_ID missing or empty")
        return 1
    if not run_date or not validate_run_date(run_date):
        log_structured(
            event="aggregation_start",
            error="RUN_DATE missing or invalid (use YYYY-MM-DD)",
            run_id=run_id,
        )
        return 1

    log_structured(
        event="aggregation_start",
        run_id=run_id,
        run_date=run_date,
        overwrite=AGG_OVERWRITE,
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

        s3_client = get_s3_client()

        # Discover canonical processed RR artifacts
        candidates = discover_processing_artifacts(conn, run_id)
        discovered_ids = {c["record_id"] for c in candidates}

        # Contract rule: if no canonical processed RR artifacts exist for run_id, fail (exit 1)
        if not discovered_ids:
            upsert_service_run(
                cur,
                run_id,
                status="failed",
                notes="no canonical processed RR artifacts found for aggregation",
                set_started=True,
                set_ended=True,
            )
            conn.commit()
            log_structured(event="aggregation_no_processed_rr_artifacts", run_id=run_id)
            return 1

        try:
            filter_ids = resolve_record_filters()
        except ValueError as e:
            upsert_service_run(cur, run_id, status="failed", notes=str(e)[:500], set_started=False, set_ended=True)
            conn.commit()
            log_structured(event="aggregation_filter_invalid", run_id=run_id, detail=str(e))
            return 1

        if filter_ids is None:
            record_list = sorted(discovered_ids)
        else:
            record_list = sorted(discovered_ids.intersection(filter_ids))
            if not record_list:
                # Same as processing: empty intersection with filters → failed, exit 1 (user misconfiguration)
                upsert_service_run(
                    cur,
                    run_id,
                    status="failed",
                    notes="record filter did not match any processing artifacts",
                    set_started=False,
                    set_ended=True,
                )
                conn.commit()
                log_structured(event="aggregation_no_records_after_filter", run_id=run_id)
                return 1

        # Keep only candidates that are in record_list (preserve uri)
        candidate_map = {c["record_id"]: c["uri"] for c in candidates if c["record_id"] in set(record_list)}
        skipped_ids: list[str] = []
        compute_ids: list[str] = []
        for rid in record_list:
            out_key = curated_output_key(run_date, run_id, rid)
            if not AGG_OVERWRITE and prefix_exists(s3_client, MINIO_BUCKET, out_key):
                skipped_ids.append(rid)
                log_structured(
                    event="aggregation_skip_record",
                    run_id=run_id,
                    record_id=rid,
                    reason="object_exists",
                )
            else:
                compute_ids.append(rid)

        if not compute_ids:
            notes = (
                f"records_total={len(record_list)},records_ok=0,records_failed=0,"
                f"records_skipped={len(skipped_ids)},windows_total=0"
            )
            upsert_service_run(
                cur,
                run_id,
                status="succeeded",
                notes=notes,
                set_started=True,
                set_ended=True,
            )
            conn.commit()
            log_structured(event="aggregation_skip", run_id=run_id, reason="all_records_exist", scope="record")
            print(
                "All selected records skipped because curated output prefixes exist. "
                "If this was a failed/partial output, rerun with AGG_OVERWRITE=1.",
                flush=True,
            )
            return 0

        paths = [s3a_path(MINIO_BUCKET, candidate_map[rid]) for rid in compute_ids]

        # service_runs: running
        upsert_service_run(cur, run_id, status="running", notes="aggregation curated windows v1", set_started=True, set_ended=False)
        conn.commit()

        # Spark: read rr_intervals, compute 5-minute curated window features
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        spark = build_spark_session()
        try:
            df = spark.read.parquet(*paths)
        except Exception as e:
            exc_class = type(e).__name__
            upsert_service_run(
                cur,
                run_id,
                status="failed",
                notes=f"Spark read failed: {exc_class}: {str(e)[:380]}",
                set_started=False,
                set_ended=True,
            )
            conn.commit()
            log_structured(event="aggregation_spark_read_failed", run_id=run_id, error=str(e))
            return 1

        # Valid RR only; rr_ms in milliseconds and deterministic 5-minute tumbling windows.
        df_valid = df.filter(F.col("rr_interval_sec").isNotNull()).withColumn(
            "rr_ms", F.col("rr_interval_sec") * 1000
        )
        df_valid = df_valid.filter(F.col("t_peak_sec").isNotNull()).withColumn(
            "window_start_sec",
            F.floor(F.col("t_peak_sec") / F.lit(300.0)) * F.lit(300.0),
        ).withColumn(
            "window_end_sec",
            F.col("window_start_sec") + F.lit(300.0),
        )
        # Successive differences are computed per record AND per window.
        w = Window.partitionBy("run_id", "record_id", "window_start_sec").orderBy("beat_index")
        df_valid = df_valid.withColumn("diff_ms", F.col("rr_ms") - F.lag("rr_ms", 1).over(w))

        # Per-window curated HRV features.
        curated_df = df_valid.groupBy(
            "run_id", "run_date", "record_id", "window_start_sec", "window_end_sec"
        ).agg(
            F.avg("rr_ms").alias("mean_rr_ms"),
            F.stddev_samp("rr_ms").alias("sdnn_ms"),
            F.sqrt(F.avg(F.col("diff_ms") * F.col("diff_ms"))).alias("rmssd_ms"),
            F.avg(F.when(F.abs(F.col("diff_ms")) > 50, 1).otherwise(0)).alias("pnn50"),
            F.count(F.col("diff_ms")).alias("n_rr"),
            F.min("t_peak_sec").alias("window_first_peak_sec"),
            F.max("t_peak_sec").alias("window_last_peak_sec"),
        ).withColumn(
            "window_valid",
            F.col("n_rr") >= F.lit(30),
        ).withColumn(
            "window_coverage_sec",
            F.col("window_last_peak_sec") - F.col("window_first_peak_sec"),
        ).withColumn(
            "window_is_partial",
            F.col("window_coverage_sec") < F.lit(300.0 - 1e-6),
        ).drop(
            "window_first_peak_sec",
            "window_last_peak_sec",
        )

        # Write one curated dataset prefix per record and register curated artifacts.
        window_counts = {
            row["record_id"]: int(row["count"])
            for row in curated_df.groupBy("record_id").count().collect()
        }
        windows_total = int(sum(window_counts.values()))
        records_with_windows = sorted(window_counts.keys())

        try:
            for record_id in compute_ids:
                if window_counts.get(record_id, 0) == 0:
                    continue
                out_key = curated_output_key(run_date, run_id, record_id)
                out_path = s3a_path(MINIO_BUCKET, out_key)
                (
                    curated_df
                    .filter(F.col("record_id") == F.lit(record_id))
                    .write
                    .mode("overwrite")
                    .parquet(out_path)
                )
                cur.execute(
                    """
                    INSERT INTO artifacts (run_id, record_id, layer, artifact_type, uri, schema_ver)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, record_id, layer, artifact_type)
                    DO UPDATE SET uri = EXCLUDED.uri, schema_ver = EXCLUDED.schema_ver, created_at = NOW()
                    """,
                    (run_id, record_id, "curated", "window_features_v1", out_key, "window_features_v1"),
                )
        except Exception as e:
            exc_class = type(e).__name__
            upsert_service_run(
                cur,
                run_id,
                status="failed",
                notes=f"Spark write failed: {exc_class}: {str(e)[:380]}",
                set_started=False,
                set_ended=True,
            )
            conn.commit()
            log_structured(event="aggregation_spark_write_failed", run_id=run_id, error=str(e))
            return 1

        n_ok = len(compute_ids)
        notes = (
            f"records_total={len(record_list)},records_ok={n_ok},records_failed=0,"
            f"records_skipped={len(skipped_ids)},records_with_windows={len(records_with_windows)},"
            f"windows_total={windows_total}"
        )
        upsert_service_run(cur, run_id, status="succeeded", notes=notes, set_started=False, set_ended=True)
        conn.commit()

        log_structured(
            event="aggregation_end",
            run_id=run_id,
            status="succeeded",
            records_total=len(record_list),
            records_ok=n_ok,
            records_failed=0,
            records_skipped=len(skipped_ids),
            records_with_windows=len(records_with_windows),
            windows_total=windows_total,
            overwrite="true" if AGG_OVERWRITE else "false",
        )
        return 0

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
        log_structured(event="aggregation_end", run_id=run_id, status="failed", error=str(e))
        return 1
    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
