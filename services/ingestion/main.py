"""
Ingestion service (Path A) – run registration only.
Reads RUN_ID, RUN_DATE from env; writes one row to runs (running → succeeded/failed); exits.
"""
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

# -----------------------------------------------------------------------------
# Config from env (use host 'postgres' in Docker network)
# -----------------------------------------------------------------------------
RUN_ID = os.environ.get("RUN_ID")
RUN_DATE = os.environ.get("RUN_DATE")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecg_metadata")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "ecg")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")


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


def main() -> int:
    if not RUN_ID or not RUN_ID.strip():
        log_structured(event="ingestion_start", error="RUN_ID missing or empty")
        return 1
    if not RUN_DATE or not validate_run_date(RUN_DATE.strip()):
        log_structured(event="ingestion_start", error="RUN_DATE missing or invalid (use YYYY-MM-DD)", run_id=RUN_ID)
        return 1

    log_structured(event="ingestion_start", run_id=RUN_ID, run_date=RUN_DATE)

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
            VALUES (%s, %s, 'running', 'ingestion v0.1 Path A')
            ON CONFLICT (run_id) DO UPDATE SET
                run_date = EXCLUDED.run_date,
                status   = 'running',
                notes    = EXCLUDED.notes
            """,
            (RUN_ID.strip(), RUN_DATE.strip()),
        )
        conn.commit()

        # Path A: no Parquet, no MinIO – just mark succeeded
        cur.execute(
            "UPDATE runs SET status = 'succeeded' WHERE run_id = %s",
            (RUN_ID.strip(),),
        )
        conn.commit()

        log_structured(event="ingestion_end", run_id=RUN_ID, status="succeeded")
        return 0

    except Exception as e:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE runs SET status = 'failed', notes = %s WHERE run_id = %s",
                    (str(e)[:500], RUN_ID.strip()),
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
