CREATE TABLE IF NOT EXISTS alerts_anomalies (
  id BIGSERIAL PRIMARY KEY,
  alert_id TEXT NOT NULL,
  source TEXT NOT NULL,           -- stock / social
  key TEXT NOT NULL,              -- symbol / topic
  event_id TEXT,                  -- can be null if derived from window
  event_ts TIMESTAMPTZ NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  metric_name TEXT NOT NULL,      -- price / mentions
  actual_value DOUBLE PRECISION NOT NULL,
  expected_value DOUBLE PRECISION,
  score DOUBLE PRECISION NOT NULL, -- e.g., z-score magnitude
  anomaly_type TEXT NOT NULL,     -- spike / drop / outlier
  details_json JSONB NOT NULL,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_id ON alerts_anomalies (alert_id);
CREATE INDEX IF NOT EXISTS idx_alerts_source_ts ON alerts_anomalies (source, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_key_ts ON alerts_anomalies (source, key, event_ts DESC);
