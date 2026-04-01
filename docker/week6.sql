CREATE TABLE IF NOT EXISTS batch_minute_aggregates (
  window_start TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  key TEXT NOT NULL,
  count BIGINT NOT NULL,
  avg_value DOUBLE PRECISION NOT NULL,
  min_value DOUBLE PRECISION NOT NULL,
  max_value DOUBLE PRECISION NOT NULL,
  computed_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (window_start, source, key)
);

CREATE INDEX IF NOT EXISTS idx_batch_source_window ON batch_minute_aggregates (source, window_start);

CREATE TABLE IF NOT EXISTS stream_batch_comparison (
  window_start TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  key TEXT NOT NULL,

  stream_count BIGINT,
  batch_count BIGINT,
  count_diff BIGINT,
  count_diff_pct DOUBLE PRECISION,

  stream_avg DOUBLE PRECISION,
  batch_avg DOUBLE PRECISION,
  avg_diff DOUBLE PRECISION,
  avg_diff_pct DOUBLE PRECISION,

  stream_min DOUBLE PRECISION,
  batch_min DOUBLE PRECISION,
  min_diff DOUBLE PRECISION,

  stream_max DOUBLE PRECISION,
  batch_max DOUBLE PRECISION,
  max_diff DOUBLE PRECISION,

  status TEXT NOT NULL, -- MATCH / WARN / FAIL
  compared_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (window_start, source, key)
);

CREATE INDEX IF NOT EXISTS idx_cmp_source_window ON stream_batch_comparison (source, window_start);
