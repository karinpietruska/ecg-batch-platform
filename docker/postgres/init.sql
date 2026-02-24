-- ECG Batch Platform – metadata schema (v1)
-- Run once on first Postgres start (Docker entrypoint).

CREATE TABLE IF NOT EXISTS runs (
    run_id     TEXT PRIMARY KEY,
    run_date   DATE NOT NULL,
    status     TEXT NOT NULL,
    notes      TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS artifacts (
    id            SERIAL PRIMARY KEY,
    run_id        TEXT NOT NULL,
    record_id     TEXT NOT NULL,
    layer         TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    uri           TEXT NOT NULL,
    schema_ver    TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quality_metrics (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL,
    record_id   TEXT NOT NULL,
    sampling_hz INT,
    n_samples   BIGINT,
    n_channels  INT,
    atr_exists  BOOLEAN,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Unique business keys to prevent duplicate artifacts / QC rows
CREATE UNIQUE INDEX IF NOT EXISTS ux_artifacts_business_key
ON artifacts (run_id, record_id, layer, artifact_type);

CREATE UNIQUE INDEX IF NOT EXISTS ux_quality_metrics_business_key
ON quality_metrics (run_id, record_id);

-- Referential integrity: artifacts and quality_metrics must reference an existing run
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_artifacts_run'
  ) THEN
    ALTER TABLE artifacts
      ADD CONSTRAINT fk_artifacts_run
      FOREIGN KEY (run_id) REFERENCES runs(run_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_quality_metrics_run'
  ) THEN
    ALTER TABLE quality_metrics
      ADD CONSTRAINT fk_quality_metrics_run
      FOREIGN KEY (run_id) REFERENCES runs(run_id);
  END IF;
END $$;
