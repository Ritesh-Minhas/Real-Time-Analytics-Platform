CREATE TABLE IF NOT EXISTS silver_clean_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  source TEXT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  key TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  attributes_json JSONB NOT NULL,
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_silver_event_id ON silver_clean_events (event_id);
CREATE INDEX IF NOT EXISTS idx_silver_source_ts ON silver_clean_events (source, event_ts);
CREATE INDEX IF NOT EXISTS idx_silver_source_key_ts ON silver_clean_events (source, key, event_ts);

CREATE TABLE IF NOT EXISTS gold_minute_aggregates (
  window_start TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  key TEXT NOT NULL,
  count BIGINT NOT NULL,
  avg_value DOUBLE PRECISION NOT NULL,
  min_value DOUBLE PRECISION NOT NULL,
  max_value DOUBLE PRECISION NOT NULL,
  last_updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (window_start, source, key)
);

CREATE INDEX IF NOT EXISTS idx_gold_source_window ON gold_minute_aggregates (source, window_start);
CREATE INDEX IF NOT EXISTS idx_gold_source_key_window ON gold_minute_aggregates (source, key, window_start);
