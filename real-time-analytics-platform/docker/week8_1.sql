ALTER TABLE evaluation_runs
  ADD COLUMN IF NOT EXISTS method TEXT DEFAULT 'ALL';

CREATE INDEX IF NOT EXISTS idx_eval_source_method_latest
  ON evaluation_runs (source, method, created_ts DESC);