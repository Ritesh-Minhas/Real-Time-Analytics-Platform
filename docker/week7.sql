CREATE TABLE IF NOT EXISTS dlq_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT,
  source TEXT,
  event_ts TIMESTAMPTZ,
  kafka_topic TEXT NOT NULL,
  kafka_partition INT,
  kafka_offset BIGINT,
  error_type TEXT NOT NULL,
  error_message TEXT NOT NULL,
  payload_json JSONB,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_created ON dlq_events (created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_source ON dlq_events (source, created_ts DESC);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
  id BIGSERIAL PRIMARY KEY,
  metric_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metric_name TEXT NOT NULL,
  metric_value DOUBLE PRECISION NOT NULL,
  labels_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metrics_ts ON pipeline_metrics (metric_ts DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON pipeline_metrics (metric_name, metric_ts DESC);
