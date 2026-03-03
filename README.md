# ECG Batch Platform

Batch-based data pipeline for ECG time-series: ingestion (WFDB/MIT-BIH), processing (R-peak, RR-interval), aggregation (HRV features), and ML-ready outputs. Services run as CLI-first containers; orchestration via Docker Compose.

---

## Architecture overview

- **Layer 0 – Orchestration** → batch run coordination (creates run identity, triggers ingestion, processing, aggregation). See `orchestrator/` and `docs/architecture_diagram.png`. The orchestrator is optional during development; services can be run manually via CLI for testing.
- **Layer 1 – Microservices** → **Ingestion** writes raw Parquet and populates **`runs`**; **Processing** discovers raw artifacts, writes `rr_intervals` Parquet and populates **`processing_metrics`** and **`service_runs`**; **Aggregation (Spark)** discovers processing artifacts, writes run-level features Parquet and populates **`features_metrics`** and **`service_runs`**.

**Status ownership:** `runs.status` is owned by ingestion. `service_runs.status` is owned by processing and aggregation. The **`artifacts`** table is the lineage backbone: it links run_id, record_id, layer, and artifact location so downstream services know what to read. Downstream services never assume file paths; they discover inputs exclusively via `artifacts`.

**Run-level records:**

- `runs`: one row per `run_id`, created/updated by ingestion.
- `service_runs`: one row per service per `run_id` (processing, aggregation, etc.).

```
Ingestion → artifacts → Processing → artifacts → Aggregation
      |                       |                         |
     runs                service_runs              service_runs
                               ↓                         ↓
                       processing_metrics        features_metrics
```

**Design guarantees**

- Storage-first idempotency at each layer
- Deterministic record selection via filter intersection
- Explicit overwrite flags per service
- Separation of control-plane (`runs`, `service_runs`) and data-plane (MinIO Parquet)

---

## Data lake layout (MinIO)

Object paths under the configured bucket (default `ecg-datalake`):

| Layer        | Path pattern |
|-------------|----------------------------------------------------------------|
| Raw         | `raw/run_date=.../run_id=.../record_id=.../ecg.parquet` |
| Processing  | `processing/run_date=.../run_id=.../record_id=.../rr_intervals.parquet` |
| Aggregation | `aggregation/run_date=.../run_id=.../features.parquet/` |

(Spark writes aggregation as a directory containing part files; all objects share that prefix.)

---

## Quick start (local)

**Prerequisites:** Docker and Docker Compose. Services use **Python 3.11+** (pinned in service Dockerfiles when added).

1. **Clone and go to the project root.**

2. **Create your environment file (no secrets committed):**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set at least `POSTGRES_PASSWORD` and `MINIO_ROOT_PASSWORD` for local use.

3. **Start the stack:**
   ```bash
   docker compose up -d
   ```
   This starts:
   - **PostgreSQL** on port 5432 (metadata: runs, artifacts, quality_metrics, service_runs, processing_metrics, features_metrics).
   - **MinIO** on ports 9000 (API) and 9001 (console).
   - **minio-bootstrap**: creates the bucket from `MINIO_BUCKET` (default `ecg-datalake`) on first run.

4. **Check that everything is up:**
   ```bash
   docker compose ps
   ```
   All services should be running. Optional: open MinIO console at http://localhost:9001 and confirm the bucket exists.

5. **Stop when done:**
   ```bash
   docker compose down
   ```

---

## Full pipeline example

Run all three Layer 1 services for the same run identity, then verify:

```bash
# 1. Ingestion
RUN_ID=demo_01 RUN_DATE=2026-03-10 docker compose run --rm ingestion

# 2. Processing
RUN_ID=demo_01 RUN_DATE=2026-03-10 docker compose run --rm processing

# 3. Aggregation
RUN_ID=demo_01 RUN_DATE=2026-03-10 docker compose run --rm aggregation
```

Check status for the run:

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT service, status FROM service_runs WHERE run_id='demo_01';"

docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, status FROM runs WHERE run_id='demo_01';"
```

(Ingestion writes to `runs`; processing and aggregation write to `service_runs`. Use synthetic mode by default; for WFDB, set `USE_SYNTHETIC_DATA=false` and ensure MIT-BIH data is mounted.)

Alternatively, you can run the full pipeline via the Layer 0 orchestrator:

```bash
cd ~/projects/ecg-batch-platform
RUN_DATE=2026-03-16 RECORD_IDS=100,101 \
./scripts/run_orchestrator.sh
```

---

## Current state & next extensions

**Current state:**

- Infrastructure (Postgres + MinIO + bucket bootstrap) is operational.
- **Ingestion** service is implemented: synthetic or WFDB mode, per-record idempotency, run status in `runs`; raw Parquet in `raw/`.
- **Processing** service is implemented: discovers raw artifacts, runs R-peak detection (NeuroKit2) and RR-interval extraction, writes `rr_intervals_v1` Parquet to `processing/`, populates `processing_metrics` and `service_runs` for `service='processing'`; same record-filter and idempotency semantics as ingestion.
- **Aggregation** service is implemented: discovers processing `rr_intervals` artifacts, computes per-record HRV features (mean_rr_ms, sdnn_ms, rmssd_ms, pnn50, n_rr) with Spark in local mode, writes run-level `features.parquet` to `aggregation/`, populates `features_metrics` and `artifacts` and `service_runs` for `service='aggregation'`; same record-filter semantics; run-level idempotency via `AGG_OVERWRITE`.

**Next extensions:**

- Add batch orchestrator

---

## Principle of least exposure (security / configuration)

- **`.env`** is **gitignored**. Never commit real credentials.
- **`.env.example`** is committed with placeholder variable names only (no secrets).
- The **dataset folder** (`data/`, e.g. local MIT-BIH download) is **gitignored**.
- **Secrets** are provided only via environment variables or local files (e.g. `.env`), not hardcoded.
- MinIO bucket name is configurable via **`MINIO_BUCKET`** so environments can differ without code changes.

---

## Project layout

Layout follows the architecture in `docs/architecture_diagram.png`: Layer 0 = orchestration, Layer 1 = microservices. Runtime infrastructure (Postgres + MinIO) is started via Docker Compose.

```
├── .env.example            # Example env vars (copy to .env)
├── docker-compose.yml       # Stack: Postgres + MinIO + minio-bootstrap (+ service definitions)
│
├── orchestrator/            # Layer 0 – orchestration & control
│
├── services/                # Layer 1 – microservices
│   ├── ingestion/           # Layer 1 – raw data ingestion → raw/ + runs
│   ├── processing/         # Layer 1 – signal processing (RR extraction) → processing/ + processing_metrics
│   └── aggregation/        # Layer 1 – feature aggregation (Spark HRV) → aggregation/ + features_metrics
│
├── docker/                  # Infrastructure configuration (Postgres schema, etc.)
│   └── postgres/
│       └── init.sql         # PostgreSQL metadata schema (runs, artifacts, service_runs, processing_metrics, features_metrics, …)
│
└── scripts/                 # Optional utilities
```

**Runtime infrastructure:** PostgreSQL (metadata DB) and MinIO (S3 data lake) are started via `docker compose`; MinIO has no repo folder—bucket and paths are configured in compose and `.env`.

---

## Dataset (for later steps)

For ingestion with real data: download the MIT-BIH Arrhythmia Database into a local folder and mount it into the ingestion container. Example:

- Host path: `./data/mitdb` (gitignored)
- Container: `/data/mitdb`
- The compose file already mounts `./data/mitdb` → `/data/mitdb` for the ingestion service.
- By default, `WFDB_LOCAL_DIR` inside the container is `/data/mitdb`. You can override it via `.env` if needed.

See the next section for concrete ingestion run examples.

---

## How to run ingestion

Ingestion runs as an **on-demand CLI container**. The general pattern is:

```bash
RUN_ID=<your_run_id> \
RUN_DATE=<yyyy-mm-dd> \
[other env vars...] \
docker compose run --rm ingestion
```

### 1. Synthetic mode (no real data required)

This is the easiest way to test the wiring (Postgres + MinIO + Parquet layout) without having MIT-BIH locally.

Minimal example (one synthetic record):

```bash
RUN_ID=2026-02-27_synth01 \
RUN_DATE=2026-02-27 \
docker compose run --rm ingestion
```

Behavior:

- Uses **synthetic data** by default (`USE_SYNTHETIC_DATA=true`).
- Writes one Parquet file to MinIO under:
  - `raw/run_date=<RUN_DATE>/run_id=<RUN_ID>/record_id=synthetic_001/ecg.parquet`
- Registers:
  - One `runs` row with status `succeeded` (unless errors occur).
  - One `artifacts` row (`layer=raw`, `artifact_type=ecg`).
  - One `quality_metrics` row (sampling rate, number of samples, channels, etc.).

You can verify the run in Postgres:

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, run_date, status FROM runs ORDER BY created_at DESC LIMIT 5;"
```

#### Multiple synthetic records

You can control which records are ingested via:

- `RECORD_IDS` – comma-separated list (takes precedence over range).
- `RECORD_RANGE` – inclusive numeric range (`start-end`, e.g. `100-124`).
- `RECORD_LIMIT` – optional cap, applied after IDs/range are resolved.

Examples:

```bash
# Explicit list of synthetic IDs
RUN_ID=2026-02-27_synth_list \
RUN_DATE=2026-02-27 \
RECORD_IDS=synthetic_001,synthetic_002,synthetic_003 \
docker compose run --rm ingestion

# Range (numbers) – mostly useful for WFDB, but also supported here
RUN_ID=2026-02-27_synth_range \
RUN_DATE=2026-02-27 \
RECORD_RANGE=100-105 \
RECORD_LIMIT=3 \
docker compose run --rm ingestion
```

Run status semantics:

- `succeeded` – no records failed (skipped due to idempotency still count as success).
- `partial_success` – at least one record succeeded, and at least one failed.
- `failed` – all records failed or the record list itself could not be parsed.

### 2. WFDB mode (MIT-BIH records)

Prerequisites:

- MIT-BIH dataset downloaded into `./data/mitdb` on the host.
- `docker compose up -d` has been run at least once so the ingestion volume mount exists.

Basic example (one real record, full length):

```bash
RUN_ID=2026-02-27_wfdb_100_full \
RUN_DATE=2026-02-27 \
USE_SYNTHETIC_DATA=false \
RECORD_IDS=100 \
docker compose run --rm ingestion
```

Behavior:

- Reads WFDB record `100` from `WFDB_LOCAL_DIR` (default `/data/mitdb`).
- Writes Parquet with columns:
  - `run_id`, `run_date`, `record_id`, `sample_index`, `t_sec`, `lead_0`, `lead_1`.
- Populates `quality_metrics` with:
  - `sampling_hz` (from WFDB `fs`),
  - `n_samples` (after any slicing),
  - `n_channels` (1 or 2; single-channel records are padded with zeros in `lead_1`),
  - `atr_exists` (whether `<record_id>.atr` is present).

You can check metrics for a run:

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT record_id, sampling_hz, n_samples, n_channels, atr_exists
 FROM quality_metrics
 WHERE run_id='2026-02-27_wfdb_100_full';"
```

#### WFDB with slicing (DEV_SLICE_SECONDS)

For faster development, you can ingest only the first N seconds of each record:

```bash
RUN_ID=2026-02-27_wfdb_100_slice10 \
RUN_DATE=2026-02-27 \
USE_SYNTHETIC_DATA=false \
DEV_SLICE_SECONDS=10 \
RECORD_IDS=100 \
docker compose run --rm ingestion
```

- `DEV_SLICE_SECONDS` is interpreted as an integer number of seconds.
- `n_samples` in `quality_metrics` is derived from the sliced array, so it matches the actual number of rows in Parquet.
- `DEV_SLICE_SECONDS <= 0` or unset means “no slicing”.

#### Multiple WFDB records

Same knobs as synthetic mode:

```bash
# Specific MIT-BIH records
RUN_ID=2026-02-27_wfdb_list \
RUN_DATE=2026-02-27 \
USE_SYNTHETIC_DATA=false \
RECORD_IDS=100,101,102 \
docker compose run --rm ingestion

# Range of MIT-BIH records, limited to first 5
RUN_ID=2026-02-27_wfdb_range \
RUN_DATE=2026-02-27 \
USE_SYNTHETIC_DATA=false \
RECORD_RANGE=100-124 \
RECORD_LIMIT=5 \
docker compose run --rm ingestion
```

Run status (`succeeded` / `partial_success` / `failed`) follows the same rules as in synthetic mode.

### 3. Idempotency and overwrite behavior

Idempotency is controlled by `INGEST_OVERWRITE`:

- `INGEST_OVERWRITE=false` (default):
  - If the target Parquet object already exists in MinIO for a given `run_id` + `record_id`, ingestion:
    - Skips regeneration/upload for that record.
    - Logs `event='ingestion_skip' reason='object_exists'`.
    - Leaves existing database rows up to date from the last successful run.
- `INGEST_OVERWRITE=true`:
  - Existing objects are overwritten.
  - Corresponding `artifacts` / `quality_metrics` rows are upserted with the new metadata.

Example overwrite run:

```bash
RUN_ID=2026-02-27_wfdb_100_full \
RUN_DATE=2026-02-27 \
USE_SYNTHETIC_DATA=false \
INGEST_OVERWRITE=true \
RECORD_IDS=100 \
docker compose run --rm ingestion
```

This makes it safe to rerun a batch with the same `RUN_ID` while still being explicit when you intend to overwrite existing raw artifacts.

---

## How to run processing

Processing runs as an **on-demand CLI container**. It discovers which records to process by querying the **artifacts** table for the given `run_id` (layer `raw`, artifact_type `ecg`), then optionally applies the same record filters as ingestion.

**Prerequisite:** At least one ingestion run must have completed for that `run_id` so raw Parquet artifacts exist in MinIO.

General pattern:

```bash
RUN_ID=<same_run_id_as_ingestion> \
RUN_DATE=<yyyy-mm-dd> \
[RECORD_IDS=...] [RECORD_RANGE=...] [RECORD_LIMIT=...] \
[PROCESS_OVERWRITE=true] \
docker compose run --rm processing
```

### Minimal example (process all raw records for a run)

After running ingestion for a run (e.g. `2026-02-27_synth01`), run:

```bash
RUN_ID=2026-02-27_synth01 \
RUN_DATE=2026-02-27 \
docker compose run --rm processing
```

Behavior:

- Discovers all records that have raw artifacts for that `run_id`.
- For each record: downloads raw Parquet, runs R-peak detection (NeuroKit2, lead_0 only), writes `rr_intervals_v1` Parquet to `processing/run_date=.../run_id=.../record_id=.../rr_intervals.parquet`, and upserts `artifacts` and `processing_metrics`.
- Records service lifecycle in `service_runs` with `service='processing'` and status `succeeded` / `partial_success` / `failed` based on counts.

Verify processing results:

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, service, status, notes FROM service_runs WHERE service='processing' ORDER BY ended_at DESC LIMIT 5;"

docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, record_id, n_beats, mean_rr_ms, sdnn_ms FROM processing_metrics WHERE run_id='2026-02-27_synth01';"
```

### Record filters

You can limit which records are processed using the same variables as ingestion:

- `RECORD_IDS` (e.g. `100,101`) – takes precedence over `RECORD_RANGE`.
- `RECORD_RANGE` (e.g. `100-124`).
- `RECORD_LIMIT` – cap after resolving IDs/range.

The service **intersects** these with the set of records that have raw artifacts. If you provide filters and the intersection is empty, the run is treated as a validation failure (exit 1).

Example (process only records 100 and 101 for a WFDB run):

```bash
RUN_ID=2026-03-02_proc_rr_wfdb_multi \
RUN_DATE=2026-03-02 \
RECORD_IDS=100,101 \
docker compose run --rm processing
```

### Idempotency: skip vs overwrite

- **`PROCESS_OVERWRITE=false`** (default): if the processing artifact (`rr_intervals.parquet`) already exists in MinIO for a record, that record is **skipped** (logged as `processing_skip`); counts as success for run status.
- **`PROCESS_OVERWRITE=true`**: existing processing artifacts are recomputed and overwritten; DB rows are upserted.

Example overwrite:

```bash
RUN_ID=2026-02-27_synth01 \
RUN_DATE=2026-02-27 \
PROCESS_OVERWRITE=true \
docker compose run --rm processing
```

Exit code: `0` when no records failed; `1` when at least one record failed or when record filter validation fails.

---

## How to run aggregation

Aggregation runs as an **on-demand CLI container**. It discovers which records to aggregate by querying the **artifacts** table for the given `run_id` (layer `processing`, artifact_type `rr_intervals`), then optionally applies the same record filters as ingestion and processing.

**Prerequisite:** At least one processing run must have completed for that `run_id` so `rr_intervals` Parquet artifacts exist in MinIO.

General pattern:

```bash
RUN_ID=<same_run_id_as_ingestion_and_processing> \
RUN_DATE=<yyyy-mm-dd> \
[RECORD_IDS=...] [RECORD_RANGE=...] [RECORD_LIMIT=...] \
[AGG_OVERWRITE=true] \
docker compose run --rm aggregation
```

### Minimal example (aggregate all processing records for a run)

After running ingestion and processing for a run (e.g. `2026-02-27_synth01`), run:

```bash
RUN_ID=2026-02-27_synth01 \
RUN_DATE=2026-02-27 \
docker compose run --rm aggregation
```

Behavior:

- Discovers all records that have processing `rr_intervals` artifacts for that `run_id`.
- If the run-level output path `aggregation/run_date=.../run_id=.../features.parquet` already exists and `AGG_OVERWRITE=false`, the service skips the run (run-level idempotency is checked *before* record discovery and filters).
- Otherwise: reads RR-interval Parquet from MinIO via Spark, computes per-record HRV metrics (mean_rr_ms, sdnn_ms, rmssd_ms, pnn50, n_rr), writes run-level `features.parquet` to `aggregation/`, and upserts `artifacts` (layer `aggregation`, artifact_type `features`) and `features_metrics`.
- Records service lifecycle in `service_runs` with `service='aggregation'` and status `succeeded` or `failed`.

Aggregation runs Spark in **local mode** (`master=local[*]`) inside the container; no external cluster is required. HRV metrics are stored as **DOUBLE PRECISION** without rounding; formatting is deferred to downstream consumers.

Verify aggregation results:

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, service, status, notes FROM service_runs WHERE service='aggregation' ORDER BY ended_at DESC LIMIT 5;"

docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, record_id, mean_rr_ms, sdnn_ms, rmssd_ms, pnn50, n_rr FROM features_metrics WHERE run_id='2026-02-27_synth01';"
```

### Record filters

You can limit which records are aggregated using the same variables as ingestion and processing:

- `RECORD_IDS` (e.g. `100,101`) – takes precedence over `RECORD_RANGE`.
- `RECORD_RANGE` (e.g. `100-124`).
- `RECORD_LIMIT` – cap after resolving IDs/range.

The service **intersects** these with the set of records that have processing artifacts. If you provide filters and the intersection is empty, the run fails (exit 1). If no filters are set and there are no processing artifacts for the run, the service exits 0 with status `succeeded` and notes "no processing artifacts found".

Example (aggregate only records 100 and 101):

```bash
RUN_ID=2026-03-02_proc_rr_wfdb_multi \
RUN_DATE=2026-03-02 \
RECORD_IDS=100,101 \
docker compose run --rm aggregation
```

### Idempotency: skip vs overwrite

- **`AGG_OVERWRITE=false`** (default): if the run-level features path already exists in MinIO, the **entire run** is skipped (logged as `aggregation_skip`, scope=run); no record discovery or filter evaluation is performed.
- **`AGG_OVERWRITE=true`**: existing run-level features are recomputed and overwritten; `artifacts` and `features_metrics` are upserted. If you want filter validation to execute for an existing output, set `AGG_OVERWRITE=true`; otherwise the job will skip before evaluating filters.

If a run crashes mid-write and you rerun without `AGG_OVERWRITE`, it may skip due to partial outputs (any object under the prefix counts as "exists"); use `AGG_OVERWRITE=1` to force recompute.

Example overwrite:

```bash
RUN_ID=2026-02-27_synth01 \
RUN_DATE=2026-02-27 \
AGG_OVERWRITE=true \
docker compose run --rm aggregation
```

Exit code: `0` when the run completes without failure (including skip or "no processing artifacts"); `1` when record filter validation fails or Spark read/write fails.
