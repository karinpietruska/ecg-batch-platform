# Aggregation Service

**Layer – Feature engineering and ML-ready dataset construction (Spark)**

## Purpose

The aggregation service discovers canonical processed RR artifacts (`rr_intervals_v1`) registered in the `artifacts` metadata table and produces:

- curated per-record 5-minute window features (`window_features_v1`)
- ml_ready window-level features for temporal modeling (`window_features_ml_v1`)
- ml_ready record-level aggregated features (`record_features_v1`)

It is a CLI-first batch container (PySpark + S3A/MinIO) using structured JSON logging and recording lifecycle events in `service_runs` for `service='aggregation'`.

---

## Canonical outputs

### Curated (`window_features_v1`)

- Path pattern: `curated/run_date=.../run_id=.../record_id=.../window_features_v1.parquet/`
- Artifact row:
  - `layer='curated'`
  - `artifact_type='window_features_v1'`
  - `schema_ver='window_features_v1'`
  - `uri=<dataset prefix, object key only>`

Each row is one 5-minute bin with HRV metrics and quality fields including `window_valid`, `window_coverage_sec`, and `window_is_partial`.
Row grain: (run_id, record_id, window_start_sec).

### ML-ready (`record_features_v1`)

- Path pattern: `ml_ready/run_date=.../run_id=.../record_features_v1.parquet/`
- Artifact row:
  - `layer='ml_ready'`
  - `artifact_type='record_features_v1'`
  - `schema_ver='record_features_v1'`
  - `uri=<dataset prefix, object key only>`

The dataset contains exactly one row per (`run_id`, `record_id`) within a run.
Row grain: (run_id, record_id).

### ML-ready (`window_features_ml_v1`)

- Path pattern: `ml_ready/run_date=.../run_id=.../window_features_ml_v1.parquet/`
- Artifact row:
  - `layer='ml_ready'`
  - `artifact_type='window_features_ml_v1'`
  - `schema_ver='window_features_ml_v1'`
  - `uri=<dataset prefix, object key only>`

The dataset preserves 5-minute temporal structure for modeling.
Row grain: (run_id, record_id, window_start_sec).

---

## Inputs

The aggregation service consumes canonical processed artifacts:

- `processed/rr_intervals_v1`

These artifacts must be registered in the metadata catalog for the target `run_id`.

---

## Layer responsibility

- `curated` datasets represent canonical HRV feature engineering outputs.
- `ml_ready` datasets provide modeling-oriented representations derived from curated features without recomputing signal-processing metrics.

---

## Metric semantics (key points)

Key metric semantics and conventions enforced by aggregation:

- Canonical `n_rr` is RR-row count from processed `rr_intervals_v1` (`count(non-null rr_ms)`).
- `pnn*` metrics are fractions in `[0,1]` (not percentages).
- `pnn*` denominators are based on non-null successive differences (`n_diff`, internal; not exposed in `record_features_v1`).
- `window_coverage_sec` is observed beat-span inside a 5-minute bin (`max(t_peak_sec) - min(t_peak_sec)`), so windows can be `< 300s` even when well-populated.

For authoritative formulas and invariants, use `docs/CANONICAL_DATA_CONTRACT.md`.

---

## Idempotency

For dual ml_ready outputs (`record_features_v1` + `window_features_ml_v1`) when `AGG_OVERWRITE=false`:

- Skip aggregation writes only if both ml_ready prefixes exist.
- If neither exists, proceed normally.
- If exactly one exists, fail fast with explicit partial-state error.

Recovery path for partial state: rerun with `AGG_OVERWRITE=1`.

---

## Determinism notes

Aggregation follows deterministic rules defined in `docs/CANONICAL_DATA_CONTRACT.md`:

- Window assignment uses `window_start_sec = floor(t_peak_sec / 300) * 300`.
- RR differences are computed with deterministic ordering by `t_peak_sec` (with tie-breaker if needed).
- Percentile calculations use a fixed Spark approximation accuracy to ensure reproducibility.

---

## Run lifecycle (high level)

1. Discover canonical processed inputs for `run_id` from `artifacts`.
2. Apply optional record filters (`RECORD_IDS`/`RECORD_RANGE`/`RECORD_LIMIT`) by intersection.
3. Compute curated windows (`window_features_v1`) per record.
4. Compute ml_ready window representation (`window_features_ml_v1`) as a projection of curated windows plus deterministic derived columns.
5. Compute record-level ml_ready (`record_features_v1`) from curated + processed RR rollups.
6. Enforce Gate C invariant: `sum(window_n_rr) == record n_rr` fail-fast.
7. Upsert curated/ml_ready artifacts and finalize `service_runs`.

---

## Failure modes

- Missing canonical processed RR artifacts for `run_id` -> fail (exit 1).
- Invalid filters or empty filter intersection -> fail (exit 1).
- Partial ml_ready state when `AGG_OVERWRITE=false` (exactly one output exists) -> fail (exit 1).
- Spark read/write failure -> fail (exit 1).
- Gate C invariant violation -> fail (exit 1).

---

## Testing

Run contract tests from project root:

```bash
./scripts/contract_test_gate_b.sh
./scripts/contract_test_gate_c.sh
```

These tests verify schema contracts, artifact registration, deterministic behavior, and strict dual-output idempotency.
