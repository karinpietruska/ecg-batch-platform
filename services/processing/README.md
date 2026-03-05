# Processing Service

**Layer 1 – Signal Processing**

## Purpose

The processing service reads raw ECG data from the `raw/` layer and writes
RR-interval artifacts to the `processed/` layer with per-record idempotency,
structured JSON logging, and lifecycle tracking in `service_runs`.

It performs R-peak detection and RR-interval extraction using NeuroKit2
(explicit method `pantompkins1985` for reproducibility) on **lead_0** only,
and stores per-record RR metrics in `processing_metrics`.

---

## Design Principles

- **Storage-first idempotency:** object storage is the source of truth for processing artifact existence.
- **Deterministic output paths:** object keys are fully derived from run identity (`run_date`, `run_id`, `record_id`).
- **Per-record isolation:** failure of one record does not stop others; service status reflects aggregate outcome.
- **Metadata consistency** via DB uniqueness constraints and upserts; database reflects object state but does not define it.
- **Discovery from artifacts:** candidate records are always resolved from `artifacts` (layer `raw`, artifact_type `ecg`), then optionally filtered.

---

## Responsibilities

- Discover raw ECG records for a given `run_id` from the `artifacts` table.
- Optionally apply record filters via `RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`.
- For each selected record:
  - Validate that the raw Parquet artifact is present and readable.
  - Compute R-peaks on `lead_0` and derive RR intervals.
  - Write an `rr_intervals_v1` Parquet file to the `processed/` layer.
  - Register the artifact in the `artifacts` table with canonical metadata:
    - `layer='processed'`
    - `artifact_type='rr_intervals_v1'`
    - `schema_ver='rr_intervals_v1'`
    - `uri=<object key only, no scheme>`
  - Upsert per-record metrics in `processing_metrics` (e.g. `n_beats`, `mean_rr_ms`, `sdnn_ms`).
- Record a per-service lifecycle in `service_runs` for `service='processing'`.

---

## Data Model (rr_intervals_v1)

Processed object key format:

```
processed/run_date=YYYY-MM-DD/run_id=<run_id>/record_id=<record_id>/rr_intervals_v1.parquet
```

`rr_intervals_v1` Parquet schema (even when empty):

- `run_id` (string)
- `run_date` (string, `YYYY-MM-DD`)
- `record_id` (string)
- `beat_index` (int64)
- `peak_index` (int64)
- `t_peak_sec` (float64)
- `rr_interval_sec` (float64, nullable)

If no peaks are detected for a record, the file is still written with this schema and contains 0 rows.

Schema version written to the metadata database: **rr_intervals_v1**

### Determinism notes

- `t_peak_sec` is derived deterministically from peak index and sampling rate.
- `beat_index` is monotonically increasing within each record and supports deterministic downstream ordering.

---

## Configuration (Environment Variables)

**Core run identity:**

- `RUN_ID`
- `RUN_DATE`

**Infrastructure (usually from `.env` / compose):**

PostgreSQL:
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Object storage:
- `MINIO_ENDPOINT`
- `MINIO_BUCKET`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

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

## Idempotency

Before writing data, the service checks whether the target processing object key
already exists in object storage:

- If the object exists and `PROCESS_OVERWRITE=false`, the record is skipped.
- If `PROCESS_OVERWRITE=true`, the object is overwritten.

Object storage (MinIO) is the primary source of truth for raw/processing
artifact existence. Database metadata reflects object state but does not define
it.

Database rows in `artifacts` and `processing_metrics` are always written via **upserts**
(`ON CONFLICT DO UPDATE`) so that reruns can safely refresh metadata for the same
`(run_id, record_id)` pair.

Database uniqueness constraints:

- `artifacts`: `(run_id, record_id, layer, artifact_type)`
- `processing_metrics`: `(run_id, record_id)`

These keys act as safety nets to keep metadata consistent even if idempotency
checks were bypassed or misconfigured.

---

## Run Lifecycle

For each processing invocation:

1. A row is upserted into `service_runs` with `service='processing'`, `status='running'`, `started_at=NOW()`.
2. Record IDs are discovered from `artifacts` (layer `raw`, artifact_type `ecg`) for the given `run_id`.
3. Optional filters (`RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`) are applied: intersect with discovered set. If filters were provided and intersection is empty → validation failure; if no filters and no raw records → "nothing to do" success.
4. Each record is processed independently:
   - Idempotency check against object storage for the target `rr_intervals_v1.parquet` key.
   - Download raw Parquet, compute RR intervals (NeuroKit2 on lead_0), write `rr_intervals_v1` Parquet.
   - Upsert `artifacts` and `processing_metrics` in PostgreSQL.
5. After all records:
   - `succeeded` if `records_failed == 0`.
   - `partial_success` if at least one record succeeded and at least one failed.
   - `failed` if all records failed or record resolution failed.

The process exits with code:

- `0` when no records failed.
- `1` when at least one record failed (including filter validation failures).

---

## Testing

The processing service has been validated with:

- Synthetic end-to-end (wiring; metrics may be zero for sine waves).
- WFDB multi-record runs (`RECORD_IDS=100,101`) with slicing.
- Full-length WFDB record (no `DEV_SLICE_SECONDS`) for plausible `n_beats` and metrics.
- Idempotent reruns (skip and overwrite via `PROCESS_OVERWRITE`).
- Filter validation: empty intersection when filters provided → `processing_no_records_after_filter`, exit 1.
- Edge case: zero or few peaks (empty or minimal `rr_intervals` table, `mean_rr_ms`/`sdnn_ms` NULL) without crash.

---

## Failure Modes

- **Invalid record filters** (e.g. malformed `RECORD_RANGE`, or filters provided but intersection with raw artifacts empty) → service run marked `failed`, exit code 1; structured log `processing_record_filter_invalid` or `processing_no_records_after_filter`.
- **Missing or unreadable raw Parquet** → logged as `processing_record_failed`, counted toward `records_failed`; run may end as `partial_success` or `failed`.
- **Object exists and overwrite disabled** → record skipped, logged as `processing_skip` with `reason='object_exists'`; counts as success for service status.

---

## Status

This service is operational once the ingestion service and the raw layer are
available and the processing container image has been built. R-peak detection
uses NeuroKit2 with method `pantompkins1985`; metrics are stored in
`processing_metrics`. Orchestration (e.g. scheduled runs) is handled by
higher-level components.

