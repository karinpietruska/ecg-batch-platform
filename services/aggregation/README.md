# Aggregation Service

**Layer 2 – Feature aggregation (Spark)**

## Purpose

The aggregation service reads processed RR-interval artifacts from the `processing/` layer (discovered via the **artifacts** table), computes per-record HRV features using Spark, and writes a **run-level** features dataset to the `aggregation/` layer plus per-record rows in `features_metrics` and `artifacts`.

This service operates as a **CLI-first batch container** (PySpark + S3A for MinIO). It uses the same run identity and filter semantics as ingestion and processing, and records its lifecycle in `service_runs` with `service='aggregation'`.

**HRV metrics (v1):** Computes per-record time-domain HRV from `rr_interval_sec` (see [Data model](#data-model) for definitions) and writes run-level `features.parquet` plus `features_metrics` and `artifacts`.

---

## Design principles

- **Run-level output:** One Parquet dataset per run: `aggregation/run_date=.../run_id=.../features.parquet` (Spark may write multiple part files under that path).
- **Storage-first idempotency:** If that path (prefix) already exists in MinIO and `AGG_OVERWRITE=false`, the whole run is skipped.
- **Discovery from artifacts:** Candidate records come from `artifacts` where `layer='processing'` and `artifact_type='rr_intervals'`; optional filters (`RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`) are intersected with that set.
- **Option B artifacts:** One `artifacts` row per record, all pointing to the same run-level `uri`, so lineage remains per-record.

---

## Responsibilities

- Discover processing artifacts for the given `run_id` from PostgreSQL.
- Apply optional record filters (same intersection rules as processing).
- Read `rr_intervals` Parquet from MinIO via Spark S3A.
- Compute per-record HRV metrics (see below) and write run-level `features.parquet`.
- Upsert `artifacts` (layer `aggregation`, artifact_type `features`, schema_ver `features_v1`) and `features_metrics` with computed values.
- Record `service_runs` for `service='aggregation'` with status and counts.

---

## Data model

**Run-level output path:**

```
aggregation/run_date=YYYY-MM-DD/run_id=<run_id>/features.parquet/
```

(Spark writes a directory; object keys have this prefix.)

**features_v1 columns:** `run_id`, `run_date`, `record_id`, `mean_rr_ms`, `sdnn_ms`, `rmssd_ms`, `pnn50`, `n_rr`.

**HRV metric definitions** (from non-null `rr_interval_sec`; `rr_ms = rr_interval_sec * 1000`; successive difference `diff_ms = rr_ms - lag(rr_ms)` within each record, ordered by `beat_index`):

| Column       | Definition |
|-------------|------------|
| `mean_rr_ms` | Mean of `rr_ms` over non-null RR (ms). |
| `sdnn_ms`    | Sample standard deviation of `rr_ms` (ms). |
| `rmssd_ms`   | √(mean of `diff_ms²`); only non-null `diff_ms`. |
| `pnn50`      | Fraction in [0, 1] of `abs(diff_ms) > 50` over non-null `diff_ms`. |
| `n_rr`       | Count of non-null `diff_ms` (number of successive differences used). |

Records with no valid RR or only one RR get a row with NULLs where undefined (e.g. `sdnn_ms`, `rmssd_ms`, `pnn50` when `n_rr` &lt; 2).

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

This service computes HRV features from processing `rr_intervals` and writes run-level Parquet plus `features_metrics`. It is operational once processing has produced `rr_intervals` artifacts and the aggregation image is built. S3A is used to read/write MinIO.
