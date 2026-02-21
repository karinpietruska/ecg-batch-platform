# Aggregation Service

**Layer 1 – Feature Aggregation**

## Purpose

The aggregation service reads processed RR series from the `processed/` layer, computes 5-minute window-based HRV features (e.g. SDNN, RMSSD, pNN50), and writes curated and ML-ready feature tables to the `curated/` and `ml_ready/` layers.

This service operates as a **CLI-first batch container** (Spark-based jobs) and is triggered by the orchestrator with a specific `run_id` and `run_date`. It discovers which records to process by querying the **artifacts** table for that run.

---

## Responsibilities

- Read processed RR series Parquet from MinIO (for the given run)
- Compute 5-minute windowing and HRV features (e.g. mean HR, SDNN, RMSSD, pNN50)
- Write curated window-level features to MinIO
- Write ML-ready record-level features to MinIO
- Register written artifacts in PostgreSQL (including `schema_ver` for each layer)
- Update metadata and lineage for curated and ml_ready outputs
- Implement storage-first idempotency (skip or overwrite) per output type

---

## Data Model

Same key structure as other layers:

**Curated (window features):**

```
curated/run_date=YYYY-MM-DD/run_id=<run_id>/record_id=<record_id>/window_features.parquet
```

Schema version: **window_features_v1**

**ML-ready (record features):**

```
ml_ready/run_date=YYYY-MM-DD/run_id=<run_id>/record_id=<record_id>/record_features.parquet
```

Schema version: **ml_features_v1**

---

## Configuration (Environment Variables)

**Required:**

- `RUN_ID`
- `RUN_DATE`
- `MINIO_*`
- `POSTGRES_*`

**Optional:**

- `AGGREGATION_OVERWRITE=false` (overwrite existing curated/ml_ready objects if `true`)

The service discovers inputs by querying the **artifacts** table for the given `run_id` (layer `processed`); no record list is passed from the orchestrator.

---

## Idempotency

Before writing data, the service checks whether the target object keys already exist in object storage.

- If an object exists and the overwrite flag is `false`, that output is skipped.
- If the overwrite flag is `true`, the object is overwritten.

Object storage is the source of truth for artifact existence.

---

## Status

This service is part of a staged implementation approach.  
It becomes operational once the processing service and the processed layer are available.
