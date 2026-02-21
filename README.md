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

## Small testable steps

- **Step 1 (current):** Repo structure, Compose (Postgres + MinIO + bucket bootstrap), DB init, README. Validate with `docker compose up -d` and `docker compose ps`.
- **Step 2:** Document data lake layout and partitioning; expand metadata usage as needed.
- **Step 3+:** Add ingestion, then processing, then aggregation, then orchestrator (see implementation plan in the repo).

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
│   ├── ingestion/          # Step 3
│   ├── processing/         # Step 4
│   └── aggregation/       # Step 5
├── orchestrator/           # Step 6
└── scripts/                # Optional utilities
```

---

## Dataset (for later steps)

For ingestion with real data: download the MIT-BIH Arrhythmia Database into a local folder and mount it into the ingestion container. Example:

- Host path: `./data/mitdb` (gitignored)
- Container: `/data/mitdb`
- Set `WFDB_LOCAL_DIR=/data/mitdb` when running ingestion.

README will be updated when the ingestion service is added.
