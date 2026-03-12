# Ingestion Service

**Layer 1 – Data Ingestion**

## Purpose

The ingestion service is responsible for loading ECG time-series data and writing it to the `raw/` layer of the data lake in a reproducible and traceable manner.

This service operates as a **CLI-first batch container** and is typically triggered by the orchestrator with a specific `run_id` and `run_date`, but it can also be run manually.

---

## Design Principles

- **Storage-first idempotency:** object storage is the source of truth for artifact existence.
- **Deterministic output paths:** object keys are fully derived from run identity (`run_date`, `run_id`, `record_id`).
- **Per-record isolation:** failure of one record does not stop others; run status reflects aggregate outcome.
- **Metadata consistency** via DB uniqueness constraints and upserts; database reflects object state but does not define it.
- **Synthetic mode** mirrors WFDB schema so the pipeline can run and be tested without external datasets.

---

## Responsibilities

- Load ECG records from either:
  - **Synthetic generator** (no external data required), or
  - **WFDB (MIT-BIH) files** from a mounted dataset directory.
- Generate deterministic timestamps: `t_sec = sample_index / sampling_rate`
- Write raw ECG data to MinIO as Parquet using the `raw_ecg_v1` schema.
- Register written artifacts in the `artifacts` table with canonical metadata:
  - `layer='raw'`
  - `artifact_type='ecg'`
  - `schema_ver='raw_ecg_v1'`
  - `uri=<object key only, no scheme>`
- Store basic quality metrics in PostgreSQL (`quality_metrics` table)
  - Sampling rate, number of samples, number of channels, annotation presence.
- Implement storage-first idempotency (skip or overwrite per record)
  with run-level status (`succeeded`, `partial_success`, `failed`).

---

## Data Model

Each MIT-BIH record is treated as a **single session**.

Raw object key format:

```
raw/run_date=YYYY-MM-DD/run_id=<run_id>/record_id=<record_id>/ecg.parquet
```

Parquet schema (`raw_ecg_v1`):

- `run_id`
- `run_date`
- `record_id`
- `sample_index`
- `t_sec`
- `lead_0`
- `lead_1`

Schema version is written to the metadata database as: **raw_ecg_v1**

---

### Determinism notes

- `t_sec` is derived deterministically from `sample_index / sampling_rate`.
- Sample ordering is preserved through `sample_index`.
- Synthetic mode uses fixed sampling rate and deterministic signal generation for reproducible pipeline behavior.

---

## Configuration (Environment Variables)

**Core run identity (required):**

- `RUN_ID`
- `RUN_DATE`

**Infrastructure (usually from `.env` / compose):**

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MINIO_ENDPOINT`, `MINIO_BUCKET`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (mapped from MinIO root user/password)

**Record selection:**

- `RECORD_IDS` (e.g. `100,101,102`)
- or `RECORD_RANGE` (e.g. `100-124`)
- `RECORD_LIMIT` (optional cap applied after IDs/range resolution)

If both `RECORD_IDS` and `RECORD_RANGE` are set, `RECORD_IDS` takes precedence.

**Mode and source:**

- `USE_SYNTHETIC_DATA`  
  - `true` (default): use synthetic generator, ignore WFDB.
  - `false`: use WFDB loader from `WFDB_LOCAL_DIR`.
- `WFDB_LOCAL_DIR`  
  - Directory inside the container where MIT-BIH WFDB records live.  
  - Default: `/data/mitdb` (the compose file mounts `./data/mitdb` here).

**Idempotency and performance:**

- `INGEST_OVERWRITE`  
  - `false` (default): skip records whose Parquet object already exists in MinIO.
  - `true`: overwrite existing objects and upsert DB metadata.
- `DEV_SLICE_SECONDS`  
  - Optional integer; if `> 0`, only the first `fs * DEV_SLICE_SECONDS` samples are read per record in WFDB mode.
  - `<= 0` or unset: slicing disabled (full record ingested).

---

## Synthetic Data Mode

When `USE_SYNTHETIC_DATA=true`, the service generates deterministic, schema-compatible ECG-like data instead of reading WFDB files.

Properties:

- Fixed sampling rate (currently 360 Hz).
- Two leads (`lead_0`, `lead_1`) as simple sinusoids with a phase shift.
- Exactly one Parquet file per `(run_id, record_id)` under:
  - `raw/run_date=<RUN_DATE>/run_id=<RUN_ID>/record_id=<RECORD_ID>/ecg.parquet`

This allows the full pipeline to run without external datasets while keeping the same schema as WFDB-based runs.

---

## WFDB Mode (MIT-BIH)

When `USE_SYNTHETIC_DATA=false`, the service:

- Reads WFDB records using `wfdb.rdrecord(os.path.join(WFDB_LOCAL_DIR, record_id))`.
- Uses `p_signal` if present, otherwise `d_signal` cast to `float32`.
- Supports:
  - **Two-channel records:** `lead_0` = channel 0, `lead_1` = channel 1.
  - **Single-channel records:** `lead_0` = channel 0, `lead_1` = zeros; `n_channels=1`
    and a structured log (`wfdb_single_channel`) are emitted.
- Optionally slices at `DEV_SLICE_SECONDS` (when set) before computing `n_samples`.

For each record, `quality_metrics` stores:

- `sampling_hz` – WFDB sampling rate.
- `n_samples` – number of samples **after** any slicing.
- `n_channels` – 1 or 2, depending on the original WFDB record.
- `atr_exists` – `true` if `<record_id>.atr` is present next to the signal.

---

## Idempotency

Before writing data, the service checks whether the target object key already exists in object storage.

- If the object exists and `INGEST_OVERWRITE=false`, the record is skipped.
- If `INGEST_OVERWRITE=true`, the object is overwritten.

Object storage (MinIO) is the primary source of truth for raw artifact existence.
Database metadata reflects object state but does not define it.

Database rows in `artifacts` and `quality_metrics` are always written via **upserts**
(`ON CONFLICT DO UPDATE`) so that reruns can safely refresh metadata for the same
`(run_id, record_id)` pair.

Database uniqueness constraints:

- `artifacts`: `(run_id, record_id, layer, artifact_type)`
- `quality_metrics`: `(run_id, record_id)`

These keys act as safety nets to keep metadata consistent even if idempotency
checks were bypassed or misconfigured.

---

## Run Lifecycle

For each ingestion invocation:

1. A row is upserted into `runs` with `status='running'`.
2. Record IDs are resolved from `RECORD_IDS` / `RECORD_RANGE`, with `RECORD_LIMIT`
   optionally applied on top.
3. Each record is processed independently:
   - Idempotency check against object storage for the target Parquet key.
   - Load either synthetic or WFDB data (depending on `USE_SYNTHETIC_DATA`).
   - Write one Parquet file into the `raw/` layer.
   - Upsert `artifacts` and `quality_metrics` in PostgreSQL.
4. After all records:
   - `succeeded` if `records_failed == 0`.
   - `partial_success` if at least one record succeeded and at least one failed.
   - `failed` if all records failed or record resolution itself failed.

The process exits with code:

- `0` when no records failed.
- `1` when at least one record failed (including record list parse failures).

This gives deterministic, observable state transitions at the run level.

---

## Testing

The ingestion service has been validated with:

- Multi-record ingestion via both `RECORD_IDS` and `RECORD_RANGE` (with `RECORD_LIMIT`).
- Idempotent reruns using both skip (`INGEST_OVERWRITE=false`) and overwrite modes.
- Partial-success scenarios with mixed success and failure across records.
- WFDB-based runs against real MIT-BIH data mounted into the container.
- Development-time slicing via `DEV_SLICE_SECONDS` in WFDB mode.
- Single-channel WFDB records using the zero-padded second lead fallback.

---

## Failure Modes

- **Invalid `RECORD_RANGE`** (e.g. malformed or start > end) → run marked `failed`, exit code 1; structured log `record_list_parse_failed`.
- **Missing or invalid WFDB record** → logged as `ingestion_record_failed`, counted toward `records_failed`; run may end as `partial_success` or `failed`.
- **Object exists and overwrite disabled** → record skipped, logged as `ingestion_skip` with `reason='object_exists'`; counts as success for run status.

---

## Status

This service is operational once the storage layer and metadata database are available and
the ingestion container image has been built. Orchestration (e.g. scheduled runs)
is handled by higher-level components.
