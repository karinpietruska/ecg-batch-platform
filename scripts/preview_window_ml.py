#!/usr/bin/env python3
"""
Preview rows from ml_ready/window_features_ml_v1 for one run + record.

Intended usage from project root (inside processing container for dependencies):

docker compose run --rm processing python scripts/preview_window_ml.py \
  --run-id mitdb_demo_01 \
  --run-date 2026-03-06 \
  --record-id 100 \
  --limit 3
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview window_features_ml_v1 rows for a run and record."
    )
    parser.add_argument("--run-id", required=True, help="Pipeline run_id")
    parser.add_argument("--run-date", required=True, help="Pipeline run_date (YYYY-MM-DD)")
    parser.add_argument("--record-id", required=True, help="Record ID to preview")
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="Maximum number of rows to print (default: 3)",
    )
    return parser.parse_args()


def list_parquet_keys(s3: Any, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys.extend(
            obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")
        )
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def main() -> int:
    args = parse_args()

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    bucket = os.environ.get("MINIO_BUCKET", "ecg-datalake")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        print(
            "ERROR: Missing AWS credentials in environment "
            "(AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY).",
            file=sys.stderr,
        )
        return 1

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    prefix = (
        f"ml_ready/run_date={args.run_date}/run_id={args.run_id}/"
        "window_features_ml_v1.parquet/"
    )
    keys = list_parquet_keys(s3, bucket, prefix)
    if not keys:
        print(f"No parquet files found under prefix: {prefix}", file=sys.stderr)
        return 1

    rows: list[dict[str, Any]] = []
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        table = pq.read_table(pa.BufferReader(obj["Body"].read()))
        rows.extend(
            row
            for row in table.to_pylist()
            if str(row.get("run_id")) == args.run_id and str(row.get("record_id")) == args.record_id
        )

    if not rows:
        print(
            "No rows found for requested filters: "
            f"run_id={args.run_id}, record_id={args.record_id}",
            file=sys.stderr,
        )
        return 1

    rows = sorted(rows, key=lambda r: int(r["window_start_sec"]))
    rows = rows[: max(0, args.limit)]

    columns = [
        "window_start_sec",
        "n_rr",
        "mean_rr_ms",
        "sdnn_ms",
        "rmssd_ms",
        "pnn50",
        "heart_rate_bpm",
        "window_valid",
    ]
    print(" | ".join(columns))
    for row in rows:
        values = [str(row.get(col)) for col in columns]
        print(" | ".join(values))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
