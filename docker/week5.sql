CREATE TABLE IF NOT EXISTS evaluation_runs (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL, -- stock/social
  throughput_events BIGINT NOT NULL,
  eps DOUBLE PRECISION NOT NULL,
  latency_p50_seconds DOUBLE PRECISION,
  latency_p95_seconds DOUBLE PRECISION,
  tp BIGINT NOT NULL,
  fp BIGINT NOT NULL,
  fn BIGINT NOT NULL,
  precision DOUBLE PRECISION,
  recall DOUBLE PRECISION,
  f1 DOUBLE PRECISION,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  details_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_latest ON evaluation_runs (source, created_ts DESC);
