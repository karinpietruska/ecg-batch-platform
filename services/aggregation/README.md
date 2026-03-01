# Aggregation Service

**Layer 1 – Feature aggregation (Spark)**

## Purpose

The aggregation service reads processed RR-interval artifacts from the `processing/` path (discovered via the **artifacts** table), computes per-record HRV features using Spark, and writes a **run-level** features dataset to the `aggregation/` path plus per-record rows in `features_metrics` and `artifacts`.

This service operates as a **CLI-first batch container** (PySpark + S3A for MinIO). It uses the same run identity and filter semantics as ingestion and processing, and records its lifecycle in `service_runs` with `service='aggregation'`.

**HRV metrics (v1):** Computes per-record time-domain HRV from `rr_interval_sec` (see [Data model](#data-model) for definitions) and writes run-level `features.parquet` plus `features_metrics` and `artifacts`.

---

## Architecture context

This service is a **Layer 1** microservice (with ingestion and processing). Discovery is via `artifacts`; services do not assume file paths.

```
ingestion → artifacts → processing → artifacts → aggregation (Spark)
      |                       |                         |
     runs                service_runs              service_runs
                               ↓                         ↓
                       processing_metrics        features_metrics
```

---

## Design principles

- **Spark execution:** Spark runs in local mode (`master=local[*]`) inside the container; no cluster is implied.
- **Run-level output:** One Parquet dataset per run: `aggregation/run_date=.../run_id=.../features.parquet` (Spark may write multiple part files under that path).
- **Storage-first idempotency:** If that path (prefix) already exists in MinIO and `AGG_OVERWRITE=false`, the whole run is skipped.
- **Discovery from artifacts:** Candidate records come from `artifacts` where `layer='processing'`, `artifact_type='rr_intervals'` (processing writes `schema_ver='rr_intervals_v1'`); optional filters (`RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`) are intersected with that set.
- **Per-record lineage to run-level dataset (Option B):** One `artifacts` row per record, all pointing to the same run-level `uri`, so lineage remains per-record.

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

RR intervals are assumed to be ordered by `beat_index` within each `record_id` prior to metric computation (ordering is guaranteed by the processing layer; aggregation does not re-sort).

| Column       | Definition |
|-------------|------------|
| `mean_rr_ms` | Mean of `rr_ms` over non-null RR (ms). |
| `sdnn_ms`    | Sample standard deviation of `rr_ms` (ms). |
| `rmssd_ms`   | √(mean of `diff_ms²`); only non-null `diff_ms`. |
| `pnn50`      | `count(abs(diff_ms) > 50) / count(non-null diff_ms)`, returned as a fraction in [0, 1] (not percentage). |
| `n_rr`       | Count of non-null successive differences (`diff_ms`) used in HRV computations (equals number of valid RR intervals − 1 when at least two exist). |

Records with no valid RR or only one RR get a row with NULLs where undefined (e.g. `sdnn_ms`, `rmssd_ms`, `pnn50` when `n_rr` &lt; 2).

Metric values are written as DOUBLE PRECISION without rounding; formatting/rounding is deferred to downstream consumers.

---

## Configuration (environment variables)

**Core:** `RUN_ID`, `RUN_DATE`

**Infrastructure:** `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`; `MINIO_ENDPOINT`, `MINIO_BUCKET`; `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (MinIO credentials).

**Record selection:** `RECORD_IDS`, `RECORD_RANGE`, `RECORD_LIMIT` (same semantics as processing; intersection with discovered processing artifacts). If filters are set and the intersection is empty → run fails. If no filters are set and there are no processing artifacts → run succeeds with "no processing artifacts found".

**Idempotency:** `AGG_OVERWRITE` — `false` (default): skip run if run-level features path exists; `true`: recompute and overwrite.

---

## Idempotency and lifecycle

- **Run-level:** If any object exists under `aggregation/run_date=.../run_id=.../features.parquet` and `AGG_OVERWRITE=false`, the service skips the run (logs `aggregation_skip`, scope=run) and exits 0. **Note:** Run-level idempotency is evaluated *before* record discovery and filter validation. If the run-level features dataset already exists and overwrite is disabled, the service skips without evaluating filters. To force filter validation or recomputation for an existing run, set `AGG_OVERWRITE=true`. **Partial output:** Existence is prefix-based (any object under the path counts). If a run crashes mid-write and you rerun without `AGG_OVERWRITE`, it may skip due to partial outputs; use `AGG_OVERWRITE=1` to force recompute.
- **service_runs:** At start, upsert `status='running'`; at end, set `status` (`succeeded` / `failed`), `ended_at`, and `notes` with counts. Exit code 0 when no records failed.
- **DB upserts:** `artifacts` and `features_metrics` are written via upserts (`ON CONFLICT DO UPDATE`). Uniqueness: `artifacts` on `(run_id, record_id, layer, artifact_type)`; `features_metrics` on `(run_id, record_id)`.

---

## Run lifecycle

1. Upsert `service_runs` with `service='aggregation'`, `status='running'`, `started_at=NOW()`.
2. If run-level features path exists in MinIO and `AGG_OVERWRITE=false` → skip run, set `status='succeeded'`, log `aggregation_skip`, exit 0. (Idempotency is checked before discovery/filters.)
3. Discover processing artifacts from `artifacts` (layer `processing`, artifact_type `rr_intervals`, schema_ver `rr_intervals_v1` from processing) for the given `run_id`.
4. Resolve optional filters (`RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`): intersect with discovered set. If filters provided and intersection empty → `status='failed'`, log `aggregation_no_records_after_filter`, exit 1. If no filters and no processing artifacts → `status='succeeded'`, notes "no processing artifacts found", log `aggregation_no_processing_records`, exit 0.
5. Read RR-interval Parquet from MinIO via Spark S3A, compute HRV per record, write run-level `features.parquet`.
6. Upsert `artifacts` (layer `aggregation`, artifact_type `features`) and `features_metrics`; update `service_runs` to `status='succeeded'` with counts. Exit 0. On Spark read/write failure → `status='failed'`, exit 1.

---

## Failure modes

- **Run row missing:** Aggregation assumes the run row exists (created by ingestion). If missing, FK constraints will cause the service to fail.
- **Invalid record filters** (e.g. malformed `RECORD_RANGE`) → `aggregation_filter_invalid`, exit 1. **Filters provided but intersection with processing artifacts empty** → `aggregation_no_records_after_filter`, `status='failed'`, exit 1.
- **No processing artifacts** for the run (ingestion only, or empty run) → `aggregation_no_processing_records`, `status='succeeded'`, exit 0; no Spark run, no features written.
- **Spark read or write failure** (e.g. missing Parquet, S3A error) → `status='failed'`, log `aggregation_spark_read_failed` or `aggregation_spark_write_failed`, exit 1.
- **Output already exists and overwrite disabled** → run skipped, log `aggregation_skip` reason=`object_exists`, exit 0. If this was a failed or partial write (e.g. some part files but no `_SUCCESS`), rerun with `AGG_OVERWRITE=1` to force recompute.

---

## Performance characteristics

- Spark runs in local mode; parallelism is bounded by container CPU allocation.
- Aggregation reads all candidate RR artifacts for a run in a single Spark job.
- Memory requirements scale with total RR rows for the run.

---

## Testing

From the project root, run the aggregation CLI test script:

```bash
./scripts/aggregation_tests.sh
```

The script validates: full pipeline (ingestion → processing → aggregation) with DB checks; idempotency (skip, then overwrite, then skip); invalid `RECORD_IDS` (filter empty intersection → failed); run with only ingestion (no processing artifacts → succeeded, no Spark); empty run (manual run row, no artifacts → no_processing_records, succeeded); artifact consistency (aggregation artifact count = features_metrics count, single run-level URI).

## Status

This service computes HRV features from processing `rr_intervals` and writes run-level Parquet plus `features_metrics`. It is operational once processing has produced `rr_intervals` artifacts and the aggregation image is built. S3A is used to read/write MinIO.
