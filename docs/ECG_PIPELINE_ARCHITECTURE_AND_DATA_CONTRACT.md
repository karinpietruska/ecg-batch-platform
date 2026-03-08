# ECG Pipeline Architecture and Data Contract

## 1) System Overview

This system implements a reproducible batch pipeline for ECG feature engineering and machine learning preparation. It ingests ECG waveform data (MIT-BIH/WFDB or synthetic mode), derives RR interval series, computes 5-minute HRV windows, and produces ML-ready features at different row grains.

Pipeline stages:
- `ingestion` -> `raw`
- `processing` -> `processed/rr_intervals_v1`
- `aggregation` -> `curated/window_features_v1`, `ml_ready/record_features_v1`, and (contract extension target) `ml_ready/window_features_ml_v1`

Deterministic timestamps and deterministic windowing rules are used to support reproducibility across reruns.

## 2) Documentation Scope

Two documentation layers are used:

- **Normative specification:** `docs/CANONICAL_DATA_CONTRACT.md`  
  Defines canonical schemas, artifact naming, aggregation rules, deterministic constants, and invariants.
- **Architecture overview (this document):**  
  Describes system components, infrastructure, operational behavior, and how the canonical contract is applied.

If wording differs, the canonical contract is the authoritative source.

## 3) High-Level Architecture

Execution flow:

```text
orchestrator
      |
      v
ingestion
      |
      v
processing
      |
      v
aggregation
   ├─ curated/window_features_v1
   ├─ ml_ready/window_features_ml_v1
   └─ ml_ready/record_features_v1
```

Core infrastructure:
- Docker Compose for local deployment
- MinIO (S3-compatible object storage) for data lake layers
- PostgreSQL for metadata and lineage
- Spark (local mode) in aggregation for batch feature computation

Microservice boundaries are explicit: each service owns its stage and communicates via object storage + metadata records.

## 4) Data Lake Layout

| Layer | Purpose | Example path |
|---|---|---|
| `raw` | Original ECG waveform data | `raw/run_date=.../run_id=.../record_id=.../ecg.parquet` |
| `processed` | RR intervals extracted from ECG | `processed/run_date=.../run_id=.../record_id=.../rr_intervals_v1.parquet` |
| `curated` | 5-minute window HRV features | `curated/run_date=.../run_id=.../record_id=.../window_features_v1.parquet/` |
| `ml_ready` | ML-ready feature representations | `ml_ready/run_date=.../run_id=.../record_features_v1.parquet/` and `ml_ready/run_date=.../run_id=.../window_features_ml_v1.parquet/` |

Notes:
- Partitioning is explicit in object keys (`run_date`, `run_id`, `record_id` where applicable).
- Spark outputs are dataset prefixes (directory-style with part files).
- Artifact URIs in PostgreSQL store object keys (no URI scheme).

## 5) Metadata and Lineage Model

Primary metadata tables:
- `runs`: run-level identity and ingestion-owned run state
- `service_runs`: lifecycle state per service per run (`running`, `succeeded`, `failed`, etc.)
- `artifacts`: lineage registry linking logical artifacts to object storage paths

Artifact linkage keys:
- `run_id`
- `record_id`
- `layer`
- `artifact_type`
- `schema_ver`
- `uri`

Discovery pattern:
- Downstream services discover inputs from `artifacts` instead of hardcoded file scanning.

## 6) Artifact Naming and Versioning

Canonical versioned artifacts:
- `rr_intervals_v1`
- `window_features_v1`
- `window_features_ml_v1`
- `record_features_v1`

Versioning rule:
- Version is encoded in `artifact_type` and mirrored in `schema_ver`.
- Canonical writes require `schema_ver == artifact_type`.

This enables backward-compatible schema evolution (for example `_v2`) without breaking existing runs.

## 7) Dataset Categories

### 7.1 `rr_intervals_v1`
Processed RR interval series derived from R-peak detection, including deterministic timing columns (for example `t_peak_sec`) and ordering fields (for example `beat_index`).

### 7.2 `window_features_v1`
Per-record 5-minute HRV windows with core metrics (`mean_rr_ms`, `sdnn_ms`, `rmssd_ms`, `pnn50`, `n_rr`) and quality columns (`window_valid`, `window_coverage_sec`, `window_is_partial`).

### 7.3 `record_features_v1`
Flattened record-level ML table containing:
- core HRV features
- derived ratio/rate features
- RR distribution and irregularity features
- temporal stability rollups
- quality/coverage rollups

Full column lists and formulas are defined in `docs/CANONICAL_DATA_CONTRACT.md`.

### 7.4 `window_features_ml_v1` (contract extension target)
Window-level ML table with one row per (`run_id`, `record_id`, `window_start_sec`), derived from curated window features plus deterministic derived columns (`heart_rate_bpm`, `rr_cv`, `rmssd_sdnn_ratio`) using denominator safety guards.

Intended use:
- primary temporal representation for arrhythmia-style and temporally sensitive modeling
- direct alignment with 5-minute window semantics

## 8) Aggregation Semantics

Aggregation service behavior:
- Reads canonical processed RR series for selected records.
- Assigns 5-minute windows via deterministic bucketing.
- Computes window metrics in curated output.
- Computes record-level rollups in ML-ready output.
- Writes (extension target) window-level ML-ready representation by projecting curated windows and adding deterministic derived columns.

Key semantic points:
- 5-minute windowing uses `window_start_sec = floor(t_peak_sec / 300) * 300`.
- Record-level weighted rollups use per-window RR counts where specified.
- Distribution and irregularity metrics are computed from RR-series values (not from window means).
- Coverage metrics are derived from the observed R-peak time span within each window.

## 9) Deterministic Processing Rules

Determinism controls include:
- `t_sec = sample_index / sampling_rate` in ingestion
- deterministic window assignment in aggregation
- deterministic diff ordering with tie-breakers for RR differences
- fixed percentile approximation accuracy in Spark

Canonical constants are defined in the canonical contract (for example `MIN_RR_PER_WINDOW`, `PERCENTILE_APPROX_ACCURACY`).

## 10) Idempotency and Reproducibility

Overwrite controls:
- `INGEST_OVERWRITE`
- `PROCESS_OVERWRITE`
- `AGG_OVERWRITE`

Idempotency model:
- Object storage existence checks determine whether datasets are skipped or recomputed.
- For dual ml_ready outputs, strict idempotency semantics are defined in the canonical contract:
  - skip only when both ml_ready outputs exist
  - fail fast on partial ml_ready state when overwrite is disabled

Reproducibility is supported by:
- deterministic processing rules
- versioned artifact contracts
- run identity (`RUN_ID`, `RUN_DATE`)
- containerized execution

## 11) Data Quality and Validation

Quality signals include:
- RR counts (`n_rr`)
- window validity flags (`window_valid`)
- coverage rollups (`mean_window_coverage_sec`, `min_window_coverage_sec`)
- partial-window ratio (`partial_window_fraction`)

Validation mechanisms include:
- invariant checks in aggregation (for example `sum(window.n_rr) == record n_rr`)
- contract test scripts (`contract_test_gate_a.sh`, `contract_test_gate_b.sh`, `contract_test_gate_c.sh`)

## 12) Reliability, Scalability, Maintainability

Reliability:
- lineage-backed artifact discovery
- explicit run/service lifecycle tracking
- invariant checks and contract tests

Scalability:
- object storage layered design
- Spark-based batch aggregation
- partitioned object paths by run and record

Maintainability:
- microservice separation of concerns
- versioned contracts
- containerized runtime and consistent local reproducibility

## 13) Security and Data Governance

Current governance and security controls:
- secrets are environment-based (`.env`) and not hardcoded
- metadata stores object keys and lineage, not embedded credentials
- schema-versioned artifacts support contract governance and auditability
- open research dataset source (MIT-BIH/PhysioNet) with explicit provenance in docs

## 14) Known Limitations

Current implementation scope:
- Spark runs in local mode (no distributed cluster orchestration in this phase)
- scheduler behavior (for example weekly cron) is deployment configuration, not hardcoded enforcement
- synthetic mode is default for reproducible local demos
- small-file optimization/compaction is out of scope for this academic phase

## 15) Future Extensions: Streaming Pipeline

A streaming extension can coexist with the batch architecture by adding:
- event ingestion (for example Kafka)
- stream processing for near-real-time feature computation
- serving/feature store for online inference consumers

Batch remains the canonical retraining and backfill path; streaming can supply low-latency features using compatible schema/version contracts.

## 16) Contract Conformance and Change Control

Contract change policy:
- introduce new dataset versions via `_v2` naming
- keep prior versions readable for backward compatibility
- update implementation, tests, and documentation together

Any contract change must update:
- `docs/CANONICAL_DATA_CONTRACT.md`
- affected service logic
- contract tests
- architecture documentation references
