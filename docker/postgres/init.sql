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

  -- service_runs: per-service lifecycle/status per run (ingestion, processing, aggregation)
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'service_runs'
  ) THEN
    CREATE TABLE service_runs (
        id         SERIAL PRIMARY KEY,
        run_id     TEXT NOT NULL,
        service    TEXT NOT NULL,
        status     TEXT NOT NULL,
        notes      TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        started_at TIMESTAMPTZ,
        ended_at   TIMESTAMPTZ
    );

    CREATE UNIQUE INDEX ux_service_runs_run_service
    ON service_runs (run_id, service);

    CREATE INDEX idx_service_runs_service_status
    ON service_runs (service, status);

    ALTER TABLE service_runs
      ADD CONSTRAINT fk_service_runs_run
      FOREIGN KEY (run_id) REFERENCES runs(run_id);
  END IF;

  -- processing_metrics: per-record metrics produced by the processing service
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'processing_metrics'
  ) THEN
    CREATE TABLE processing_metrics (
        id           SERIAL PRIMARY KEY,
        run_id       TEXT NOT NULL,
        record_id    TEXT NOT NULL,
        n_beats      INT,
        mean_rr_ms   DOUBLE PRECISION,
        sdnn_ms      DOUBLE PRECISION,
        created_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE UNIQUE INDEX ux_processing_metrics_business_key
    ON processing_metrics (run_id, record_id);

    ALTER TABLE processing_metrics
      ADD CONSTRAINT fk_processing_metrics_run
      FOREIGN KEY (run_id) REFERENCES runs(run_id);
  END IF;

  -- features_metrics: per-record HRV features produced by the aggregation service (Spark)
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'features_metrics'
  ) THEN
    CREATE TABLE features_metrics (
        id           SERIAL PRIMARY KEY,
        run_id       TEXT NOT NULL,
        record_id    TEXT NOT NULL,
        mean_rr_ms   DOUBLE PRECISION,
        sdnn_ms      DOUBLE PRECISION,
        rmssd_ms     DOUBLE PRECISION,
        pnn50        DOUBLE PRECISION,
        n_rr         INT,
        created_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE UNIQUE INDEX ux_features_metrics_business_key
    ON features_metrics (run_id, record_id);

    ALTER TABLE features_metrics
      ADD CONSTRAINT fk_features_metrics_run
      FOREIGN KEY (run_id) REFERENCES runs(run_id);
  END IF;
END $$;
