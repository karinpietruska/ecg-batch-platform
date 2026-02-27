# Processing Service

**Layer 1 – Signal Processing**

## Purpose

The processing service reads raw ECG data from the `raw/` layer and writes
RR-interval artifacts to the `processing/` layer in a way that mirrors the
ingestion service: per-record idempotency, structured logging, and explicit
service-level status in `service_runs`.

In the initial skeleton (v0.1), the service does **not** perform R-peak
detection yet. It validates wiring by creating **empty-but-typed**
`rr_intervals_v1` artifacts (0 rows, final schema) for each record.

---

## Responsibilities (skeleton v0.1)

- Discover raw ECG records for a given `run_id` from the `artifacts` table.
- Optionally apply record filters via `RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`.
- For each selected record:
  - Validate that the raw Parquet artifact is present and readable.
  - Write an empty `rr_intervals_v1` Parquet file to the `processing/` layer.
  - Register the artifact in the `artifacts` table (`layer='processing'`).
  - Upsert a placeholder row in `processing_metrics`.
- Record a per-service lifecycle in `service_runs` for `service='processing'`.

Future versions will replace the empty table with real RR-interval content and
populated metrics.

---

## Data Model (rr_intervals_v1)

Processed object key format:

```
processing/run_date=YYYY-MM-DD/run_id=<run_id>/record_id=<record_id>/rr_intervals.parquet
```

`rr_intervals_v1` Parquet schema (even when empty):

- `run_id` (string)
- `run_date` (string)
- `record_id` (string)
- `beat_index` (int64)
- `peak_index` (int64)
- `t_peak_sec` (float64)
- `rr_interval_sec` (float64, nullable)

Schema version written to the metadata database: **rr_intervals_v1**

---

## Configuration (Environment Variables)

**Core run identity:**

- `RUN_ID`
- `RUN_DATE`

**Infrastructure (usually from `.env` / compose):**

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MINIO_ENDPOINT`, `MINIO_BUCKET`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

**Record selection:**

- `RECORD_IDS` (e.g. `100,101,102`)
- or `RECORD_RANGE` (e.g. `100-124`)
- `RECORD_LIMIT` (optional cap after IDs/range)

If both `RECORD_IDS` and `RECORD_RANGE` are set, `RECORD_IDS` takes precedence.

The service always discovers candidate inputs from `artifacts` for the given
`run_id` (`layer='raw'`, `artifact_type='ecg'`) and then intersects with any
filters provided. If filters are provided but the intersection is empty, the
run is treated as a validation failure.

**Idempotency:**

- `PROCESS_OVERWRITE`  
  - `false` (default): skip records whose processing artifact already exists.
  - `true`: overwrite existing processing artifacts and upsert DB metadata.

---

## Idempotency & Lifecycle

Before writing data, the service checks whether the target processing object key
already exists in object storage:

- If the object exists and `PROCESS_OVERWRITE=false`, the record is skipped.
- If `PROCESS_OVERWRITE=true`, the object is overwritten.

Object storage (MinIO) is the primary source of truth for raw/processing
artifact existence. Database metadata reflects object state but does not define
it.

Per-service lifecycle is tracked in `service_runs`:

- At start: upsert `service_runs` with `service='processing'`,
  `status='running'`, `started_at=NOW()`.
- At end: update the same row with `status` (`succeeded` / `partial_success` /
  `failed`), `ended_at=NOW()`, and a `notes` string summarizing counts.

Exit code:

- `0` when no records failed.
- `1` when at least one record failed or when filters are invalid.

---

## Status

This service becomes operational once the ingestion service and the raw layer
are available and the processing image has been built. Future versions will
add real R-peak detection and RR-interval metrics on top of this skeleton.

