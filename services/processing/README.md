# Processing Service

**Layer 1 – Signal Processing**

## Purpose

The processing service reads raw ECG data from the `raw/` layer, performs R-peak detection and RR-interval extraction with quality control, and writes processed RR series to the `processed/` layer.

This service operates as a **CLI-first batch container** and is triggered by the orchestrator with a specific `run_id` and `run_date`. It discovers which records to process by querying the **artifacts** table for that run.

---

## Responsibilities

- Read raw ECG Parquet from MinIO (for the given run)
- Perform R-peak detection and RR-interval extraction
- Apply quality control and compute QC metrics
- Write processed RR series to MinIO as Parquet
- Register written artifacts in PostgreSQL (including `schema_ver`)
- Store QC metrics and lineage in the metadata database
- Implement storage-first idempotency (skip or overwrite)

---

## Data Model

Processed object key format (same structure as raw):

```
processed/run_date=YYYY-MM-DD/run_id=<run_id>/record_id=<record_id>/rr_series.parquet
```

Schema version written to the metadata database: **rr_series_v1**

(Exact Parquet columns are defined in the service; `run_id`, `run_date`, `record_id` are included for self-describing data.)

---

## Configuration (Environment Variables)

**Required:**

- `RUN_ID`
- `RUN_DATE`
- `MINIO_*`
- `POSTGRES_*`

**Optional:**

- `PROCESSING_OVERWRITE=false` (overwrite existing processed objects if `true`)

The service discovers inputs by querying the **artifacts** table for the given `run_id` (layer `raw`); no record list is passed from the orchestrator.

---

## Idempotency

Before writing data, the service checks whether the target object key already exists in object storage.

- If the object exists and `PROCESSING_OVERWRITE=false`, the record is skipped.
- If `PROCESSING_OVERWRITE=true`, the object is overwritten.

Object storage is the source of truth for artifact existence.

---

## Status

This service is part of a staged implementation approach.  
It becomes operational once the ingestion service and the raw layer are available.
