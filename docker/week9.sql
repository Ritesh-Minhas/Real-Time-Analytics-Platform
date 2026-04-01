ALTER TABLE alerts_anomalies
ADD COLUMN IF NOT EXISTS severity_score DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_alert_severity
ON alerts_anomalies (severity_score DESC);