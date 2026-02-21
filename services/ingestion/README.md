# Ingestion Service

**Layer 1 – Data Ingestion**

## Purpose

The ingestion service is responsible for loading ECG time-series data and writing it to the `raw/` layer of the data lake in a reproducible and traceable manner.

This service operates as a **CLI-first batch container** and is triggered by the orchestrator with a specific `run_id` and `run_date`.

---

## Responsibilities

- Load ECG records (MIT-BIH dataset, local WFDB files)
- Generate deterministic timestamps: `t_sec = sample_index / sampling_rate`
- Write raw ECG data to MinIO as Parquet
- Register written artifacts in PostgreSQL (including `schema_ver`)
- Store basic quality metrics (e.g. sampling rate, number of samples)
- Implement storage-first idempotency (skip or overwrite)

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

## Configuration (Environment Variables)

**Required:**

- `RUN_ID`
- `RUN_DATE`
- `MINIO_*`
- `POSTGRES_*`

**Record selection:**

- `RECORD_IDS` (e.g. `100,101,102`)
- or `RECORD_RANGE` (e.g. `100-124`)

**Optional:**

- `WFDB_SOURCE=local`
- `WFDB_LOCAL_DIR=/data/mitdb`
- `USE_SYNTHETIC_DATA=true`
- `DEV_SLICE_SECONDS=60`
- `INGEST_OVERWRITE=false`

---

## Synthetic Data Mode

When `USE_SYNTHETIC_DATA=true`, the service generates deterministic, schema-compatible ECG-like data instead of reading WFDB files.

This allows the full pipeline to run without external dependencies.

---

## Idempotency

Before writing data, the service checks whether the target object key already exists in object storage.

- If the object exists and `INGEST_OVERWRITE=false`, the record is skipped.
- If `INGEST_OVERWRITE=true`, the object is overwritten.

Object storage is the source of truth for artifact existence.

---

## Status

This service is part of a staged implementation approach.  
It becomes operational once the storage layer and metadata database are available.
