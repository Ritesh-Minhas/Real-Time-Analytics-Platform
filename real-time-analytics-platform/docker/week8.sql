ALTER TABLE alerts_anomalies
  ADD COLUMN IF NOT EXISTS method TEXT DEFAULT 'EWMA_Z';

ALTER TABLE alerts_anomalies
  ADD COLUMN IF NOT EXISTS model_version TEXT DEFAULT 'v1';

CREATE INDEX IF NOT EXISTS idx_alerts_method_ts
  ON alerts_anomalies (method, created_ts DESC);