# ECG HRV Feature Pipeline

Reproducible batch pipeline that converts ECG recordings into heart rate variability (HRV) features and produces machine-learning-ready feature datasets.

## Overview

This project processes ECG time-series data through a layered data pipeline:

`raw ECG -> RR intervals -> HRV windows -> ML-ready feature datasets`

The system ingests ECG recordings, performs R-peak detection and RR-interval extraction, computes 5-minute HRV statistics, and writes two ML-ready outputs: a primary window-level modeling table and a secondary record-level baseline table.

The pipeline is implemented as a containerized microservice architecture. Individual services handle ingestion, signal processing, and feature aggregation, and communicate through shared storage layers. The system runs as a reproducible batch pipeline using Docker and Docker Compose.

All datasets are stored as Parquet in an S3-compatible object store (MinIO) and registered in PostgreSQL metadata tables for lineage and discovery.

## Project Goals

This project demonstrates how to design a reproducible biomedical data pipeline with:

- layered data architecture (`raw -> processed -> curated -> ml_ready`)
- explicit data contracts and schema versioning
- reproducible batch processing using containerized services
- metadata-driven artifact tracking and lineage
- contract tests that enforce pipeline invariants

## System Properties

The architecture is designed to satisfy key data engineering requirements for data-intensive systems.

**Reliability**

- Batch runs are orchestrated using a deterministic pipeline runner (`run_orchestrator.sh`).
- Artifact idempotency rules and fail-fast checks reduce the risk of partial outputs in the `ml_ready` layer.
- Pipeline stages fail fast if required inputs or outputs are missing.

**Scalability**

- Processing is implemented using Apache Spark (PySpark), enabling distributed computation if the pipeline is deployed on a cluster.
- Data artifacts are stored in partitioned Parquet datasets within S3-compatible object storage (MinIO).
- The architecture separates ingestion, processing, and aggregation services, allowing independent scaling.

**Maintainability**

- The pipeline follows a staged, service-based data architecture (`raw -> processed -> curated -> ml_ready`).
- Canonical artifact schemas and invariants are defined in explicit data contracts.
- Each pipeline stage is implemented as an isolated containerized service.

**Reproducibility**

- The full environment is defined via Docker Compose.
- All artifacts are versioned by `run_id` and `run_date`.
- Code and configuration are tracked in a Git repository.

**Data Security**

- Services run in isolated Docker containers and communicate only through controlled storage layers.
- In local deployment, MinIO and PostgreSQL are containerized services with network access controlled by deployment configuration.
- Credentials and configuration are managed through environment variables defined in the deployment environment.

**Data Governance and Lineage**

- All produced datasets are registered in PostgreSQL metadata tables.
- Artifact metadata records the layer, artifact type, schema version, and originating run.
- This enables lineage tracing from ML-ready datasets back to the original input records.

**Data Protection**

- The pipeline processes publicly available ECG research data from the MIT-BIH Arrhythmia Database.
- No personally identifiable information (PII) is processed or stored.
- Data processing therefore operates within a non-personal biomedical research context.

**Key capabilities**

- Ingest ECG recordings from the MIT-BIH Arrhythmia Database (WFDB format)
- Detect R-peaks and compute RR interval series
- Generate 5-minute HRV feature windows
- Produce ML-ready feature representations:
  - window-level temporal features for sequence-aware modeling
  - record-level aggregated baseline features
- Track data lineage and artifacts via PostgreSQL metadata
- Execute the pipeline reproducibly using containerized services

**Technologies used:** Python · Apache Spark (PySpark) · Docker · MinIO · PostgreSQL

## Data Pipeline Architecture

[![ECG Pipeline Architecture](docs/architecture_diagram.png)](docs/architecture_diagram.png)

*Click the image to view the full-resolution architecture diagram.*

```text
raw ECG
   |
   v
ingestion
   |
   v
processing
   |
   v
aggregation
   ├─ curated/window_features_v1      (HRV windows)
   ├─ ml_ready/window_features_ml_v1  (primary modeling dataset)
   └─ ml_ready/record_features_v1     (record-level baseline)
```

The window-level ML dataset (`window_features_ml_v1`) is the primary modeling representation because arrhythmia tasks operate on temporal segments rather than full-record aggregates.

### Microservices

- **Ingestion Service**: loads ECG records and writes `raw` artifacts plus ingestion metadata.
- **Processing Service**: performs R-peak detection, computes RR intervals, and writes `processed/rr_intervals_v1`.
- **Aggregation Service**: computes 5-minute HRV windows in `curated/window_features_v1` and produces ML-ready outputs in `ml_ready`.

### Storage Services

- **PostgreSQL**: stores run metadata, service lifecycle state, and artifact lineage records.
- **MinIO**: stores Parquet artifacts across `raw`, `processed`, `curated`, and `ml_ready`.

| Data stage | Purpose |
|---|---|
| `raw` | Original ECG waveform data |
| `processed` | RR intervals extracted from ECG |
| `curated` | 5-minute HRV feature windows |
| `ml_ready` | Machine-learning-ready feature datasets (window-level temporal + record-level aggregate) |

Canonical artifacts by data stage:
- `processed`: `rr_intervals_v1`
- `curated`: `window_features_v1`
- `ml_ready`: `window_features_ml_v1` (primary), `record_features_v1` (secondary)

## ML-Ready Outputs

The pipeline produces two complementary ML-ready datasets: a window-level temporal representation used for most modeling tasks, and a record-level aggregated baseline.
Both datasets are derived from curated 5-minute HRV windows and registered as versioned artifacts in the metadata catalog.

### `window_features_ml_v1` (primary temporal representation)

Path:

`ml_ready/run_date=.../run_id=.../window_features_ml_v1.parquet/`

One row per 5-minute window (`run_id`, `record_id`, `window_start_sec`), preserving within-record temporal variation for arrhythmia-style and sequence-aware modeling.
Row grain: (`run_id`, `record_id`, `window_start_sec`).

### `record_features_v1` (secondary record-level baseline representation)

Path:

`ml_ready/run_date=.../run_id=.../record_features_v1.parquet/`

Each row corresponds to one ECG record and contains aggregated HRV metrics derived from RR intervals.
Row grain: (`run_id`, `record_id`).

### Window-Level ML Features (`window_features_ml_v1`)
These features describe HRV behavior within each 5-minute window.

- `mean_rr_ms`
- `sdnn_ms`
- `rmssd_ms`
- `pnn50`
- `n_rr`
- `heart_rate_bpm`
- `rr_cv`
- `rmssd_sdnn_ratio`
- `window_valid`
- `window_coverage_sec`
- `window_is_partial`

### Record-Level Aggregated Features (`record_features_v1`)
These features summarize HRV statistics across all windows of a record.

#### Core HRV Metrics

- `mean_rr_ms`
- `sdnn_ms`
- `rmssd_ms`
- `pnn50`

#### Derived Cardiovascular Indicators

- `heart_rate_bpm`
- `rr_cv`
- `rmssd_sdnn_ratio`

#### RR Distribution Statistics

- `rr_min_ms`
- `rr_max_ms`
- `rr_range_ms`
- `rr_median_ms`
- `rr_iqr_ms`

#### Beat-to-Beat Irregularity Metrics

- `sdsd_ms`
- `pnn20`

#### Temporal Stability Across Windows

- `mean_rr_window_std`
- `sdnn_window_std`
- `rmssd_window_std`

#### Data Quality and Coverage Indicators

- `window_count_total`
- `window_count_valid`
- `valid_window_fraction`
- `mean_window_coverage_sec`
- `min_window_coverage_sec`
- `partial_window_fraction`

## Idempotency

Aggregation enforces strict dual-output idempotency semantics when `AGG_OVERWRITE=false`:

| State of ml_ready outputs | Behavior |
|---|---|
| both exist (`record_features_v1` and `window_features_ml_v1`) | skip |
| none exist | run aggregation |
| exactly one exists | fail (partial state) |

This prevents inconsistent ML-ready states where only one of the two outputs exists.

## Dataset Setup (MIT-BIH Arrhythmia Database)

### Dataset Source

This pipeline is designed to process ECG recordings from the MIT-BIH Arrhythmia Database:

- [MIT-BIH Arrhythmia Database (PhysioNet)](https://physionet.org/content/mitdb/1.0.0/)

The dataset contains 48 half-hour ambulatory ECG recordings sampled at 360 Hz and distributed in WFDB format.
The ingestion service reads WFDB files (`.dat`, `.hea`, `.atr`) using the Python WFDB library.
Some records include extra archival annotation variants (for example `102-0.atr`); the pipeline uses the standard record basename triplet (`<record>.dat`, `<record>.hea`, `<record>.atr`) and ignores extra variants.

### Download the Dataset

Download and extract the dataset from PhysioNet locally.  
The full MIT-BIH dataset contains 48 recordings, but the pipeline can be tested with a small subset.  
For an initial run, records `100`, `101`, and `102` are sufficient.

### Place the Dataset in the Expected Directory

From the repository root (`ecg-batch-platform/`), create the dataset folder:

```bash
mkdir -p data/mitdb
```

This command creates `data/mitdb/` relative to the repository root.

Place downloaded WFDB files inside:

`data/mitdb/`

Example structure:

```text
data/
  mitdb/
    100.dat
    100.hea
    100.atr
    101.dat
    101.hea
    101.atr
```

Docker Compose mounts this directory into containers as:

`/data/mitdb`

## Environment Setup

Requirements:
- Docker
- Docker Compose
- Linux or macOS shell

Optional (host-side utilities outside Docker):
- Python 3.11+

Initialize environment:

```bash
cp .env.example .env
docker compose up -d
```

## Environment Check

Before running the pipeline, confirm infrastructure containers are running:

```bash
docker compose ps
```

Expected services:
- `postgres`
- `minio`

If they are not running, start them with:

```bash
docker compose up -d
```

## Quick Start: Run the Pipeline on Real ECG Data

Use one `RUN_ID`/`RUN_DATE` consistently across all stages.
`RUN_ID` and `RUN_DATE` uniquely identify a pipeline execution and are used for dataset partitioning and metadata lineage.

Recommended first real-data run:
- `USE_SYNTHETIC_DATA=false`
- `RECORD_IDS=100,101,102`

### Run via Orchestrator (Recommended)

```bash
RUN_ID=mitdb_demo_01 \
RUN_DATE=2026-03-06 \
USE_SYNTHETIC_DATA=false \
RECORD_IDS=100,101,102 \
./scripts/run_orchestrator.sh
```

### Scheduled Execution (Example: Weekly)

For production-style operation, the pipeline orchestrator can be scheduled via a host-level scheduler such as cron.
Example (weekly, Sunday 02:00 UTC):

```cron
0 2 * * 0 cd /path/to/ecg-batch-platform && ./scripts/scheduled_orchestrator.sh
```

In practice, batch pipelines are typically triggered by an external scheduler (for example cron, a CI/CD workflow, or an orchestration platform such as Airflow). This project demonstrates the execution pattern using a cron example while keeping the pipeline itself scheduler-independent.

This project includes `scripts/scheduled_orchestrator.sh`. Scheduling cadence is configured externally (cron, CI/CD scheduler, or workflow runner) rather than inside the pipeline itself.

### Manual Execution (Optional)

```bash
RUN_ID=mitdb_demo_01 RUN_DATE=2026-03-06 USE_SYNTHETIC_DATA=false RECORD_IDS=100,101,102 docker compose run --rm ingestion
RUN_ID=mitdb_demo_01 RUN_DATE=2026-03-06 RECORD_IDS=100,101,102 docker compose run --rm processing
RUN_ID=mitdb_demo_01 RUN_DATE=2026-03-06 RECORD_IDS=100,101,102 docker compose run --rm aggregation
```

## Verify Results

### Check Service Execution

Check ingestion run status (`runs` table):

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, status FROM runs WHERE run_id='mitdb_demo_01';"
```

Check processing and aggregation service status (`service_runs` table):

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, service, status FROM service_runs WHERE run_id='mitdb_demo_01' ORDER BY service;"
```

Expected successful statuses:
- `runs.status -> succeeded` (ingestion status in `runs`)
- `processing -> succeeded`
- `aggregation -> succeeded`

### Check Produced Artifacts

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT layer, artifact_type, schema_ver, record_id FROM artifacts WHERE run_id='mitdb_demo_01' ORDER BY layer, record_id;"
```

Expected artifact coverage includes:
- `processed / rr_intervals_v1`
- `curated / window_features_v1`
- `ml_ready / window_features_ml_v1`
- `ml_ready / record_features_v1`

### Inspect Primary ML-Ready Output

To inspect the primary modeling dataset (`ml_ready/window_features_ml_v1`) for the demo run:

```bash
docker compose run --rm processing python scripts/preview_window_ml.py \
  --run-id mitdb_demo_01 \
  --run-date 2026-03-06 \
  --record-id 100 \
  --limit 3
```

This prints the first up to 3 windows for the selected record (for example `window_start_sec = 0, 300, 600` when available).

## Canonical Data Contracts

Canonical schemas, derivation rules, and invariants:
- `docs/CANONICAL_DATA_CONTRACT.md`

System architecture and technical specification:
- `docs/ECG_PIPELINE_ARCHITECTURE_AND_DATA_CONTRACT.md`

## Contract Tests (Optional)

```bash
./scripts/contract_test_gate_a.sh
./scripts/contract_test_gate_b.sh
./scripts/contract_test_gate_c.sh
```

These tests validate canonical artifact contracts, semantic checks, invariants, and idempotency behavior.

## Synthetic Data Mode (Optional)

Synthetic mode is available for quick smoke tests without downloading MIT-BIH data:

```bash
RUN_ID=synth_demo RUN_DATE=2026-03-06 ./scripts/run_orchestrator.sh
```

Synthetic mode is intended for rapid local testing; real ECG data should be used to assess semantic correctness of HRV feature computation.

## Repository Structure

```text
docker-compose.yml
.env.example

services/
  ingestion/      # WFDB ECG ingestion
  processing/     # R-peak detection and RR interval extraction
  aggregation/    # HRV window features and ML-ready datasets

docs/
  architecture_diagram.png
  ECG_PIPELINE_ARCHITECTURE_AND_DATA_CONTRACT.md
  CANONICAL_DATA_CONTRACT.md

scripts/
  run_orchestrator.sh
  preview_window_ml.py
  contract_test_gate_a.sh
  contract_test_gate_b.sh
  contract_test_gate_c.sh
```

## Potential Production Scaling

Although the current system is deployed locally using Docker Compose, the architecture is designed to scale to production environments.

In a production setting, the microservices could be deployed on a container orchestration platform such as Kubernetes.
MinIO could be replaced with a managed object storage system (for example AWS S3 or Azure Blob Storage), and PostgreSQL could run as a managed metadata service.
The processing layer implemented with PySpark could execute on a distributed Spark cluster to process significantly larger ECG datasets.
Workflow orchestration could be handled by a scheduler such as Apache Airflow or a cloud-native workflow engine.

These changes would allow the same architectural principles to scale to larger datasets and distributed compute environments while preserving the reproducible batch processing design.

## Additional Documentation

Service-specific implementation details:
- `services/ingestion/README.md`
- `services/processing/README.md`
- `services/aggregation/README.md`
