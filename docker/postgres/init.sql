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
