# Batch Orchestrator

**Layer 0 – Orchestration & Control**

## Purpose

The orchestrator is the control plane for the pipeline. It generates a unique **run_id** and **run_date** for each execution and triggers the services in order: ingestion → processing → aggregation.

It operates as a **CLI or cron-triggered job** (e.g. via Docker Compose) and passes run context to each service via environment variables.

---

## Responsibilities

- Generate **run_id** (e.g. format `YYYY-MM-DD_<8char>`) and **run_date** (ISO date) per pipeline run
- Trigger the ingestion service with `RUN_ID`, `RUN_DATE`, and record selection (`RECORD_IDS` or `RECORD_RANGE`)
- Trigger the processing service with `RUN_ID` and `RUN_DATE` (processing discovers inputs from the artifacts table)
- Trigger the aggregation service with `RUN_ID` and `RUN_DATE` (aggregation discovers inputs from the artifacts table)
- Ensure run metadata is written to PostgreSQL (e.g. via each service registering artifacts)

Typical invocation: `docker compose run --rm ingestion` (and similarly for processing, aggregation), with env vars set for the run.

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

For development, the pipeline can be triggered manually; for production, a cron container or scheduler can invoke the orchestrator at the desired frequency (e.g. weekly).

---

## Status

This component is part of a staged implementation approach.  
It becomes operational once at least the ingestion service is available, so there is a concrete job to trigger.
