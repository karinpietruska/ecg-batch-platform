# ECG Batch Platform

Reproducible batch data pipeline for ECG processing, HRV feature engineering, and ML dataset preparation.

This project converts raw ECG signals into canonical RR intervals, curated 5-minute HRV windows, and a record-level ML-ready feature table. The system is implemented as containerized services and can be executed either through the orchestrator script (recommended) or by running services manually in sequence.

The canonical submission architecture is:
- `raw` -> original ECG artifacts
- `processed` -> `rr_intervals_v1`
- `curated` -> `window_features_v1`
- `ml_ready` -> `record_features_v1`

## Architecture Overview

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
   ├─ curated/window_features_v1
   └─ ml_ready/record_features_v1
```

| Layer | Purpose |
|---|---|
| `raw` | Original ECG files |
| `processed` | RR intervals derived from ECG |
| `curated` | 5-minute HRV windows |
| `ml_ready` | Record-level ML feature table |

## Architecture Documentation

Detailed architecture and system-level behavior are documented in:

- `docs/ECG_PIPELINE_ARCHITECTURE_AND_DATA_CONTRACT.md`

Canonical schemas, artifact naming rules, aggregation formulas, and invariants are defined in:

- `docs/CANONICAL_DATA_CONTRACT.md`

## Canonical Outputs

Output object paths (MinIO/S3 style):

- `processed/run_date=.../run_id=.../record_id=.../rr_intervals_v1.parquet`
- `curated/run_date=.../run_id=.../record_id=.../window_features_v1.parquet/`
- `ml_ready/run_date=.../run_id=.../record_features_v1.parquet/`

Canonical artifact names:
- `rr_intervals_v1`
- `window_features_v1`
- `record_features_v1`

Authoritative schemas, derivation rules, and invariants are defined in `docs/CANONICAL_DATA_CONTRACT.md`.

## Prerequisites

- Docker + Docker Compose
- Linux/macOS shell

Initial setup:

```bash
cp .env.example .env
docker compose up -d
```

Default execution uses synthetic mode (`USE_SYNTHETIC_DATA=true`), which provides the fastest path for reviewers to run the pipeline locally. WFDB/MIT-BIH mode is optional.

## Reviewer Quick Run

Use one `RUN_ID`/`RUN_DATE` consistently across all stages.

Recommended (orchestrator):

```bash
RUN_ID=review_demo_01 RUN_DATE=2026-03-04 ./scripts/run_orchestrator.sh
```

Manual fallback:

```bash
RUN_ID=review_demo_01 RUN_DATE=2026-03-04 docker compose run --rm ingestion
RUN_ID=review_demo_01 RUN_DATE=2026-03-04 docker compose run --rm processing
RUN_ID=review_demo_01 RUN_DATE=2026-03-04 docker compose run --rm aggregation
```

## Expected Outcomes

| Stage | Output layer | artifact_type | Success signal |
|---|---|---|---|
| ingestion | `raw` | `ecg` | `runs.status='succeeded'` |
| processing | `processed` | `rr_intervals_v1` | `service_runs(service='processing').status='succeeded'` |
| aggregation | `curated` | `window_features_v1` | `service_runs(service='aggregation').status='succeeded'` + curated artifact rows |
| aggregation | `ml_ready` | `record_features_v1` | ml_ready artifact rows + one row per record in dataset |

## Verify Results

Run the two checks below after pipeline execution:

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT run_id, service, status, started_at IS NOT NULL AS has_started_at, ended_at IS NOT NULL AS has_ended_at FROM service_runs WHERE run_id='review_demo_01' ORDER BY service;"
```

```bash
docker compose exec postgres psql -U ecg -d ecg_metadata -c \
"SELECT layer, artifact_type, schema_ver, record_id, uri FROM artifacts WHERE run_id='review_demo_01' ORDER BY layer, record_id;"
```

Expected artifact coverage:
- `processed / rr_intervals_v1`
- `curated / window_features_v1`
- `ml_ready / record_features_v1`

## Contract Tests

Optional but recommended for reproducibility checks:

```bash
./scripts/contract_test_gate_a.sh
./scripts/contract_test_gate_b.sh
./scripts/contract_test_gate_c.sh
```

These validate:
- Gate A: canonical processed RR artifacts and negative-path behavior
- Gate B: curated window schema/semantics and idempotency
- Gate C: ml_ready schema/semantics/invariants and idempotency sentinel

## Repository Structure

```text
docker-compose.yml
.env.example

services/
  ingestion/
  processing/
  aggregation/

docs/
  ECG_PIPELINE_ARCHITECTURE_AND_DATA_CONTRACT.md
  CANONICAL_DATA_CONTRACT.md

scripts/
  run_orchestrator.sh
  contract_test_gate_a.sh
  contract_test_gate_b.sh
  contract_test_gate_c.sh
```

## Additional Documentation

- `services/ingestion/README.md`
- `services/processing/README.md`
- `services/aggregation/README.md`

Detailed implementation behavior, configuration nuances, and stage-specific failure modes are documented in these service READMEs.
