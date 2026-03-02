# Batch Orchestrator

**Layer 0 – Orchestration & Control**

## Purpose

The orchestrator is the control plane for the pipeline. It generates a unique **run_id** and **run_date** for each execution and triggers the services in order: ingestion → processing → aggregation.

It operates as a **CLI or cron-triggered job** (e.g. via Docker Compose) and passes run context to each service via environment variables.

The orchestrator fails fast: if any service returns a non-zero exit code, execution stops immediately and that exit code is returned.

The orchestrator does not inspect the database directly; it relies on each service's exit code and the `service_runs` table those services write.

---

## Responsibilities

- Generate **run_id** (e.g. format `YYYY-MM-DD_<8char>`) and **run_date** (ISO date) per pipeline run
- Trigger the ingestion service with `RUN_ID`, `RUN_DATE`, and record selection (`RECORD_IDS` or `RECORD_RANGE`)
- Trigger the processing service with `RUN_ID` and `RUN_DATE` (processing discovers inputs from the artifacts table)
- Trigger the aggregation service with `RUN_ID` and `RUN_DATE` (aggregation discovers inputs from the artifacts table)
- Ensure run metadata is written to PostgreSQL (e.g. via each service registering artifacts)

### Quick start

Start infrastructure (once):

```bash
docker compose up -d postgres minio minio-bootstrap
```

Run full pipeline (synthetic default):

```bash
docker compose run --rm orchestrator
```

Typical invocation with explicit date and record selection:

```bash
# Example: run full pipeline for a given date and record selection
RUN_DATE=2026-03-15 RECORD_IDS=100,101 \
docker compose run --rm orchestrator
```

**When aggregation skips:** If the orchestrator invokes aggregation and the service skips (output prefix already exists), surface a hint to the user: *"Skipped because output prefix exists. If this was a failed/partial output, rerun with AGG_OVERWRITE=1."* (The aggregation service itself also prints this when it skips.)

---

## What the Orchestrator Passes

| Service     | RUN_ID | RUN_DATE | RECORD_IDS / RECORD_RANGE |
|------------|--------|----------|----------------------------|
| Ingestion  | ✓      | ✓        | ✓ (required for ingestion) |
| Processing | ✓      | ✓        | — (discovers from artifacts) |
| Aggregation| ✓      | ✓        | — (discovers from artifacts) |

---

## Configuration (Environment Variables)

The orchestrator sets (or reads from its environment) and passes to each service:

- `RUN_ID` – unique identifier for this pipeline run
- `RUN_DATE` – date of the run (e.g. `YYYY-MM-DD`)
- For ingestion only: `RECORD_IDS` or `RECORD_RANGE` (and connection details for MinIO, Postgres, WFDB_LOCAL_DIR, etc.)

The orchestrator forwards record-selection and overwrite flags (e.g. `RECORD_*`, `DEV_*`, `AGG_OVERWRITE`) to downstream services unchanged.

For development, the pipeline can be triggered manually; for production, a cron container or scheduler can invoke the orchestrator at the desired frequency (e.g. weekly).

---

## Status

This component is part of a staged implementation approach.  
It becomes operational once at least the ingestion service is available, so there is a concrete job to trigger.
