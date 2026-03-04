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

**Record selection:** `RECORD_IDS`, `RECORD_RANGE`, `RECORD_LIMIT` (same semantics as processing; intersection with discovered processing artifacts). If filters are set and the intersection is empty → run fails. If canonical processed RR artifacts are missing for the run → run fails.

**Idempotency:** `AGG_OVERWRITE` — `false` (default): apply per-record skip when curated output exists; `true`: recompute and overwrite.

---

## Idempotency and lifecycle

- **service_runs:** At start, upsert `status='running'`; at end, set `status` (`succeeded` / `failed`), `ended_at`, and `notes` with counts. Exit code 0 when no records failed.
- **DB upserts:** Curated `artifacts` are written via upserts (`ON CONFLICT DO UPDATE`). Uniqueness: `artifacts` on `(run_id, record_id, layer, artifact_type)`.

### Curated idempotency semantics (`window_features_v1`)

- Idempotency is enforced per record.
- For each (`run_id`, `record_id`): if curated prefix exists and `AGG_OVERWRITE=false`, the record is skipped.
- Other records in the same run are still processed.
- If all selected records already exist and `AGG_OVERWRITE=false`, aggregation exits 0 and logs `reason='all_records_exist'`.

`window_coverage_sec` measures observed R-peak span inside each 5-minute bin (`max(t_peak_sec) - min(t_peak_sec)`). Because R-peaks rarely align exactly with bin boundaries, coverage may be `< 300s` even for well-populated windows. Downstream consumers should apply a coverage threshold (for example `>=270s` or `>=290s`) rather than expecting exact `300s`.

### ML-ready output semantics (`record_features_v1`)

- Output path: `ml_ready/run_date=.../run_id=.../record_features_v1.parquet/`.
- The dataset has exactly one row per (`run_id`, `record_id`).
- Canonical `n_rr` is RR-row count from processed `rr_intervals_v1` (`count(non-null rr_ms)`).
- `pnn*` metrics are computed over successive differences (`diff_ms`) with denominator `n_diff` (count of non-null `diff_ms`); `n_diff` is internal and not exposed in `record_features_v1`.
- Idempotency sentinel for Gate C is the ml_ready prefix: if it exists and `AGG_OVERWRITE=false`, aggregation skips both curated and ml_ready writes.

---

## Run lifecycle

1. Upsert `service_runs` with `service='aggregation'`, `status='running'`, `started_at=NOW()`.
2. Discover processing artifacts from `artifacts` (layer `processed`, artifact_type `rr_intervals_v1`, schema_ver `rr_intervals_v1`) for the given `run_id`. If none exist, fail.
3. Resolve optional filters (`RECORD_IDS` / `RECORD_RANGE` / `RECORD_LIMIT`): intersect with discovered set. If filters provided and intersection empty → `status='failed'`, log `aggregation_no_records_after_filter`, exit 1.
4. Apply per-record idempotency check on curated output prefixes when `AGG_OVERWRITE=false`; existing records are skipped.
5. Read RR-interval Parquet from MinIO via Spark S3A, compute 5-minute window features, write per-record curated `window_features_v1` datasets.
6. Upsert curated `artifacts` rows (`layer='curated'`, `artifact_type='window_features_v1'`); update `service_runs` to `status='succeeded'` with counts. Exit 0. On Spark read/write failure → `status='failed'`, exit 1.

---

## Failure modes

- **Run row missing:** Aggregation assumes the run row exists (created by ingestion). If missing, FK constraints will cause the service to fail.
- **Invalid record filters** (e.g. malformed `RECORD_RANGE`) → `aggregation_filter_invalid`, exit 1. **Filters provided but intersection with processing artifacts empty** → `aggregation_no_records_after_filter`, `status='failed'`, exit 1.
- **No canonical processed RR artifacts** for the run → `aggregation_no_processed_rr_artifacts`, `status='failed'`, exit 1.
- **Spark read or write failure** (e.g. missing Parquet, S3A error) → `status='failed'`, log `aggregation_spark_read_failed` or `aggregation_spark_write_failed`, exit 1.
- **Output already exists and overwrite disabled** → affected records are skipped (`aggregation_skip_record`). If all selected records exist, run exits 0 with `aggregation_skip` reason=`all_records_exist`. If this was a failed or partial write (e.g. some part files but no `_SUCCESS`), rerun with `AGG_OVERWRITE=1` to force recompute.

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

The script validates: full pipeline (ingestion → processing → aggregation) with DB checks; idempotency (skip, then overwrite, then skip); invalid `RECORD_IDS` (filter empty intersection → failed); missing canonical processed input (aggregation fails as expected); and curated artifact consistency (`layer='curated'`, `artifact_type='window_features_v1'`).

## Status

This service computes 5-minute HRV window features from processing `rr_intervals_v1` and writes curated per-record datasets plus curated artifact lineage. It is operational once processing has produced canonical processed RR artifacts and the aggregation image is built. S3A is used to read/write MinIO.
