CREATE TABLE IF NOT EXISTS dq_results (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  check_name TEXT NOT NULL,
  status TEXT NOT NULL,                 -- PASS / WARN / FAIL
  total_rows BIGINT NOT NULL,
  failed_rows BIGINT NOT NULL,
  details_json JSONB NOT NULL,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_latest ON dq_results (source, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_dq_window ON dq_results (window_start, window_end);
