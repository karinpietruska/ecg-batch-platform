# Canonical Data Contract

This document defines the canonical dataset schemas, artifact naming conventions,
derivation rules, and invariants used by the ECG batch processing pipeline.

It specifies the authoritative data contract for datasets written to the
processed, curated, and ml_ready stages of the data lake.

Related documentation:
- Architecture overview: `docs/ECG_PIPELINE_ARCHITECTURE_AND_DATA_CONTRACT.md`
- Pipeline run instructions: `README.md`

If descriptions differ across documents, this file defines the authoritative dataset contract.

---

## 0) Documentation scope

This document specifies the canonical dataset schemas, artifact naming conventions,
derivation rules, and invariants used in the pipeline.

Additional context is provided in:

- Architecture overview: `docs/ECG_PIPELINE_ARCHITECTURE_AND_DATA_CONTRACT.md`
- Pipeline run instructions: `README.md`

If wording differs across documents, the definitions in this file take precedence.

---

## 0.1) Contract decisions (implemented)

The following decisions are implemented and locked for this contract version:

- `ml_ready/window_features_ml_v1` is the **primary temporal representation** for modeling tasks that depend on within-record temporal variation.
- `ml_ready/record_features_v1` remains the **secondary record-level baseline representation** for tabular summary modeling.

Strict dual-output idempotency policy (for `AGG_OVERWRITE=false`) is:

- Skip aggregation writes only when **both** ml_ready output prefixes exist for the run.
- If exactly one ml_ready output exists (partial ml_ready state), aggregation must fail fast (non-zero exit) with an explicit partial-state error and recovery path (`AGG_OVERWRITE=true`).

This section codifies current implemented behavior.

---

## 1) Canonical naming (authoritative)

### 1.1 Data lake prefixes

- `raw/` (unchanged)
- `processed/`
- `curated/`
- `ml_ready/`

### 1.2 Artifact layer values

- `raw`
- `processed`
- `curated`
- `ml_ready`

### 1.3 Versioning

Version is encoded in `artifact_type` and mirrored in `schema_ver` (required by DB schema).

- `rr_intervals_v1`
- `window_features_v1`
- `window_features_ml_v1`
- `record_features_v1`

Rule:

- For canonical writes, `schema_ver` MUST equal `artifact_type`.

---

## 2) Path + metadata contract

Note: Spark outputs are written as directory prefixes containing part files; non-Spark services may write a single Parquet object.

Definition: Row grain

Row grain defines the level of detail represented by a single row in a dataset
and is typically expressed as the set of columns that uniquely identify a row.

## 2.1 Processed layer: RR intervals (`rr_intervals_v1`)

- **Path:** `processed/run_date=.../run_id=.../record_id=.../rr_intervals_v1.parquet` (single object key, not a dataset prefix)
- **Write form:** single Parquet object (non-Spark)
- **Scale note:** Per-record Parquet objects are acceptable for submission scale; production small-file optimization is out of scope.
- **Artifact row:**
  - `layer='processed'`
  - `artifact_type='rr_intervals_v1'`
  - `schema_ver='rr_intervals_v1'`
  - `uri=<object key only, no scheme>`

## 2.2 Curated layer: 5-min window features (`window_features_v1`)

- **Path:** `curated/run_date=.../run_id=.../record_id=.../window_features_v1.parquet/`
- **Write form:** Spark dataset prefix (directory containing part files)
- **Partitioning intent:** dataset prefix per (`run_id`, `record_id`); rows within the prefix are grouped by `window_start_sec`
- **Artifact row:**
  - `layer='curated'`
  - `artifact_type='window_features_v1'`
  - `schema_ver='window_features_v1'`
  - `uri=<object key only, no scheme>`

## 2.3 ML-ready layer: flattened record table (`record_features_v1`)

- **Path:** `ml_ready/run_date=.../run_id=.../record_features_v1.parquet/`
- **Write form:** Spark dataset prefix (directory containing part files)
- **Partitioning intent:** run-level dataset; one row per (`run_id`, `record_id`)
- **Row grain / key guarantee:** primary key is (`run_id`, `record_id`)
- **Artifact row:**
  - `layer='ml_ready'`
  - `artifact_type='record_features_v1'`
  - `schema_ver='record_features_v1'`
  - `uri=<object key only, no scheme>`

## 2.4 ML-ready layer: window-level modeling table (`window_features_ml_v1`)

- **Path:** `ml_ready/run_date=.../run_id=.../window_features_ml_v1.parquet/`
- **Write form:** Spark dataset prefix (directory containing part files)
- **Row grain / key guarantee:** primary key is (`run_id`, `record_id`, `window_start_sec`)
- **Artifact row:**
  - `layer='ml_ready'`
  - `artifact_type='window_features_ml_v1'`
  - `schema_ver='window_features_ml_v1'`
  - `uri=<object key only, no scheme>`

`window_start_sec` semantics:

- Integer seconds relative to record start.
- Represents the canonical 5-minute bin start (`0, 300, 600, ...`) for the window.
- Must be consistent with deterministic assignment rule in section 4.

Required columns (frozen for v1):

- **Identifiers:** `run_id`, `run_date`, `record_id`, `window_start_sec`
- **Core HRV (preserved from curated):** `mean_rr_ms`, `sdnn_ms`, `rmssd_ms`, `pnn50`, `n_rr`
- **Derived (added in ml_ready):** `heart_rate_bpm`, `rr_cv`, `rmssd_sdnn_ratio`
- **Quality (preserved from curated):** `window_valid`, `window_coverage_sec`, `window_is_partial`

Derived column safety guards (required):

- `heart_rate_bpm = 60000 / mean_rr_ms` if `mean_rr_ms > 0`, else `NULL`
- `rr_cv = sdnn_ms / mean_rr_ms` if `mean_rr_ms > 0`, else `NULL`
- `rmssd_sdnn_ratio = rmssd_ms / sdnn_ms` if `sdnn_ms > 0`, else `NULL`

v1 implementation constraint:

- `window_features_ml_v1` MUST be a projection of canonical curated `window_features_v1`
  plus deterministic derived columns only.
- It MUST NOT recompute RR metrics, alter window boundaries, or redefine curated metric validity rules.
- Curated remains the authority for metric validity (`sdnn_ms`, `rmssd_ms`, `pnn50`, `window_valid`, etc.); ml_ready preserves those fields.

### `record_features_v1` canonical columns

- **Identifiers:** `run_id`, `run_date`, `record_id`
- **Core HRV:** `mean_rr_ms`, `sdnn_ms`, `rmssd_ms`, `pnn50`, `n_rr`
- **Derived:** `heart_rate_bpm`, `rr_cv`, `rmssd_sdnn_ratio`
- **Distribution:** `rr_min_ms`, `rr_max_ms`, `rr_range_ms`, `rr_median_ms`, `rr_iqr_ms`
- **Beat irregularity:** `sdsd_ms`, `pnn20` (optional extension: `pnn10`)
- **Temporal stability across windows:** `mean_rr_window_std`, `sdnn_window_std`, `rmssd_window_std`
- **Quality/coverage rollups:** `window_count_total`, `window_count_valid`, `valid_window_fraction`, `mean_window_coverage_sec`, `min_window_coverage_sec`, `partial_window_fraction`
- **Metadata:** `created_at`
- **Type convention:** `pnn50`, `pnn20` (and optional `pnn10`), `valid_window_fraction`, `partial_window_fraction` are `DOUBLE` fractions in `[0,1]` (not percentages).

### Record-level aggregation rules (v1)

- Source: canonical curated `window_features_v1` + canonical processed `rr_intervals_v1` for the same (`run_id`, `record_id`)
- RR universe for record-level aggregation is exactly canonical `rr_intervals_v1` rows (trusted cleaned RR universe); no additional outlier filtering is applied.
- Window-to-record aggregation:
  - `mean_rr_ms`, `sdnn_ms`, `rmssd_ms`, `pnn50` use weighted mean by per-window `window_features_v1.n_rr`
  - canonical `n_rr` is `count(non-null rr_ms)` from processed RR-series values
  - invariant: `sum(window_features_v1.n_rr)` MUST equal canonical record-level `n_rr` for each (`run_id`, `record_id`)
  - if this invariant is violated, aggregation MUST fail with exit code `1`
  - `window_valid = (n_rr >= MIN_RR_PER_WINDOW)` where `MIN_RR_PER_WINDOW=30`
  - `*_window_std` are unweighted sample stddev across valid windows, where valid means `window_valid = true AND metric IS NOT NULL`
- Derived:
  - `heart_rate_bpm = 60000 / mean_rr_ms` if `mean_rr_ms > 0`, else `NULL`
  - `rr_cv = sdnn_ms / mean_rr_ms` if `mean_rr_ms > 0`, else `NULL`
  - `rmssd_sdnn_ratio = rmssd_ms / sdnn_ms` if `sdnn_ms > 0`, else `NULL`
- Distribution/irregularity MUST be computed from RR-series values (not from window means):
  - `rr_min_ms = min(rr_ms)`, `rr_max_ms = max(rr_ms)`, `rr_range_ms = rr_max_ms - rr_min_ms`
  - `rr_median_ms = percentile_approx(rr_ms, 0.5, PERCENTILE_APPROX_ACCURACY)`
  - `rr_iqr_ms = percentile_approx(rr_ms, 0.75, PERCENTILE_APPROX_ACCURACY) - percentile_approx(rr_ms, 0.25, PERCENTILE_APPROX_ACCURACY)`
  - `sdsd_ms = stddev_samp(diff_ms)` where `diff_ms = rr_ms - lag(rr_ms)` over (`PARTITION BY run_id, record_id ORDER BY t_peak_sec ASC`); if `t_peak_sec` ties are possible, apply a deterministic tie-breaker (for example `beat_index`)
  - `pnn20 = count(abs(diff_ms) > 20) / count(non-null diff_ms)` (fraction in `[0,1]`)
- Coverage rollups:
  - `window_count_total = count(windows)`
  - `window_count_valid = count(window_valid = true)`
  - `valid_window_fraction = window_count_valid / window_count_total`
  - `mean_window_coverage_sec = avg(window_coverage_sec)` over rows where `window_coverage_sec IS NOT NULL`; if none, `NULL`
  - `min_window_coverage_sec = min(window_coverage_sec)` over rows where `window_coverage_sec IS NOT NULL`; if none, `NULL`
  - `partial_window_fraction = avg(CAST(window_is_partial AS DOUBLE))` over rows where `window_is_partial IS NOT NULL`; if none, `NULL`

---

## 3) Discovery contract (canonical only)

Aggregation MUST discover RR inputs from `artifacts` where:

- `layer='processed'`
- `artifact_type='rr_intervals_v1'`
- `schema_ver='rr_intervals_v1'`

No legacy fallback is supported in this submission build.

If no canonical processed RR artifacts exist for the `run_id`, aggregation MUST fail with exit code `1`.

---

## 3.1) Dual ml_ready idempotency semantics (strict partial-state failure)

For `AGG_OVERWRITE=false`, aggregation idempotency across ml_ready outputs is defined as:

- **Skip case:** aggregation skips curated + ml_ready writes only when both ml_ready prefixes exist for the run:
  - `ml_ready/run_date=.../run_id=.../window_features_ml_v1.parquet/`
  - `ml_ready/run_date=.../run_id=.../record_features_v1.parquet/`
- **Proceed case:** if neither ml_ready prefix exists, aggregation proceeds normally.
- **Partial-state failure case:** if exactly one ml_ready prefix exists, aggregation MUST fail fast.

Partial-state failure requirements:

- Exit code MUST be `1`.
- Structured log event MUST be emitted with:
  - `event='aggregation_partial_ml_ready_state'`
  - `run_id`
  - `record_features_exists` (boolean)
  - `window_features_exists` (boolean)
  - `agg_overwrite` (boolean/string flag value)
- `service_runs.status` MUST be `failed`.
- `service_runs.notes` MUST explain partial ml_ready state (one output exists, the other missing).
- Recovery path MUST be documented and surfaced in logs: rerun with `AGG_OVERWRITE=true`.

This strict rule prevents silent skips on incomplete ml_ready output state.

---

## 4) Canonical invariants

- Deterministic window assignment: `window_start_sec = floor(t_peak_sec / 300) * 300`
- `window_start_sec` is treated as integer-second bucket start relative to record start (`0, 300, 600, ...`).
- `t_peak_sec` is the R-peak time in seconds (derived deterministically from sample index and sampling rate), stored in `rr_intervals_v1`
- Each RR row corresponds to a beat; `t_peak_sec` is the timestamp of that beat's R-peak (the beat that terminates the RR interval)
- `MIN_RR_PER_WINDOW = 30` (v1 constant)
- Window inclusion rule in v1: keep all windows; include `n_rr` and `window_valid = (n_rr >= MIN_RR_PER_WINDOW)`
- For Gate C diffs (`diff_ms`), ordering is deterministic by `t_peak_sec ASC` (with deterministic tie-breaker if needed)
- `PERCENTILE_APPROX_ACCURACY = 10000` (v1 constant); `percentile_approx` in Gate C MUST pass this value explicitly
- Idempotency flags by layer/output:
  - processed RR: `PROCESS_OVERWRITE`
  - curated windows: `AGG_OVERWRITE`
  - ml_ready record features (produced by aggregation in v1): `AGG_OVERWRITE`
- Given identical `rr_intervals_v1` input and overwrite flags, curated and ml_ready outputs are deterministic
- Record filter semantics unchanged: `RECORD_IDS`/`RECORD_RANGE`/`RECORD_LIMIT` intersect discovered IDs; empty intersection with filters -> fail (exit 1)
- No scheme in `artifacts.uri` (store object key only)
- New writes use canonical prefixes only

---

## 5) Contract conformance and change control

Schema and artifact contract changes MUST use versioned evolution (for example `_v2`) and MUST remain backward-compatible for existing stored artifacts.

Any contract version change requires synchronized updates to:
- this canonical contract
- affected service logic (ingestion/processing/aggregation as applicable)
- contract tests
- architecture narrative docs and README references

