# ECG Batch Platform

Batch-based data pipeline for ECG time-series: ingestion (WFDB/MIT-BIH), processing (R-peak, RR-interval), aggregation (HRV features), and ML-ready outputs. Services run as CLI-first containers; orchestration via Docker Compose.

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
   - **PostgreSQL** on port 5432 (metadata: runs, artifacts, quality_metrics).
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

## Current state & next extensions

**Current state:**

- Infrastructure (Postgres + MinIO + bucket bootstrap) is operational.
- Ingestion service is implemented and runnable as a standalone CLI-style container:
  - Synthetic mode (default) writes one or more Parquet ECG files to MinIO.
  - WFDB mode reads real MIT-BIH records from a mounted dataset folder.
  - Per-record idempotency with overwrite/skip behavior, plus run status tracking.

**Next extensions:**

- Add processing service (RR extraction)
- Add aggregation service (HRV features)
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

```
├── docker-compose.yml      # System entrypoint: Postgres + MinIO + bootstrap
├── .env.example            # Example env vars (copy to .env)
├── docker/
│   └── postgres/
│       └── init.sql        # Metadata schema (runs, artifacts, quality_metrics)
├── services/
│   ├── ingestion/          # Layer 1 – raw data ingestion
│   ├── processing/         # Layer 1 – signal processing (RR extraction)
│   └── aggregation/        # Layer 1 – feature computation (Spark)
├── orchestrator/           # Layer 0 – batch run coordination
└── scripts/                # Optional utilities
```

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
