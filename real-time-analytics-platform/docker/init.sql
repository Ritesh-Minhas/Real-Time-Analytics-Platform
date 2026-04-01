CREATE TABLE IF NOT EXISTS bronze_raw_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  source TEXT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  payload_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_source_ts ON bronze_raw_events (source, event_ts);
CREATE INDEX IF NOT EXISTS idx_bronze_event_id ON bronze_raw_events (event_id);
