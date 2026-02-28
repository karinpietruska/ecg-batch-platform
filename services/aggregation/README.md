# Aggregation Service

**Layer 2 – Feature aggregation (Spark)**

## Purpose

The aggregation service reads processed RR-interval artifacts from the `processing/` layer (discovered via the **artifacts** table), computes per-record HRV features using Spark, and writes a **run-level** features dataset to the `aggregation/` layer plus per-record rows in `features_metrics` and `artifacts`.

This service operates as a **CLI-first batch container** (PySpark + S3A for MinIO). It uses the same run identity and filter semantics as ingestion and processing, and records its lifecycle in `service_runs` with `service='aggregation'`.

**Skeleton (v0.1):** Currently writes a placeholder features dataset (one row per record with dummy metrics) to validate Spark ↔ MinIO (S3A) and DB wiring. Real metrics (mean_rr_ms, sdnn_ms, rmssd_ms, pnn50, n_rr) will be computed in a follow-up step.

---

## Design principles

- **Run-level output:** One Parquet dataset per run: `aggregation/run_date=.../run_id=.../features.parquet` (Spark may write multiple part files under that path).
- **Storage-first idempotency:** If that path (prefix) already exists in MinIO and `AGG_OVERWRITE=false`, the whole run is skipped.
- **Discovery from artifacts:** Candidate records come from `artifacts` where `layer='processing'` and `artifact_type='rr_intervals'`; optional filters (`RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`) are intersected with that set.
- **Option B artifacts:** One `artifacts` row per record, all pointing to the same run-level `uri`, so lineage remains per-record.

---

## Responsibilities (skeleton v0.1)

- Discover processing artifacts for the given `run_id` from PostgreSQL.
- Apply optional record filters (same intersection rules as processing).
- Read `rr_intervals` Parquet from MinIO via Spark S3A.
- Write a run-level placeholder `features.parquet` (one row per `run_id`, `record_id`).
- Upsert `artifacts` (layer `aggregation`, artifact_type `features`, schema_ver `features_v1`) and `features_metrics` (placeholder values).
- Record `service_runs` for `service='aggregation'` with status and counts.

---

## Data model

**Run-level output path:**

```
aggregation/run_date=YYYY-MM-DD/run_id=<run_id>/features.parquet/
```

(Spark writes a directory; object keys have this prefix.)

**features_v1 (placeholder) columns:** `run_id`, `run_date`, `record_id`, `mean_rr_ms`, `sdnn_ms`, `rmssd_ms`, `pnn50`, `n_rr`.

---

## Configuration (environment variables)

**Core:** `RUN_ID`, `RUN_DATE`

**Infrastructure:** `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`; `MINIO_ENDPOINT`, `MINIO_BUCKET`; `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (MinIO credentials).

**Record selection:** `RECORD_IDS`, `RECORD_RANGE`, `RECORD_LIMIT` (same semantics as processing; intersection with discovered processing artifacts).

**Idempotency:** `AGG_OVERWRITE` — `false` (default): skip run if run-level features path exists; `true`: recompute and overwrite.

---

## Idempotency and lifecycle

- **Run-level:** If any object exists under `aggregation/run_date=.../run_id=.../features.parquet` and `AGG_OVERWRITE=false`, the service skips the run (logs `aggregation_skip`, scope=run) and exits 0.
- **service_runs:** At start, upsert `status='running'`; at end, set `status` (`succeeded` / `failed`), `ended_at`, and `notes` with counts. Exit code 0 when no records failed.

---

## Status

This service is a **skeleton** for Spark-based aggregation. It is operational once processing has produced `rr_intervals` artifacts and the aggregation image is built. S3A is used to read/write MinIO; if S3A setup blocks progress, a local-mount fallback can be used instead.
