CREATE OR REPLACE VIEW dq_latest_scored AS
WITH latest AS (
  SELECT DISTINCT ON (source, check_name)
    source,
    check_name,
    status,
    total_rows,
    failed_rows,
    details_json,
    created_ts
  FROM dq_results
  ORDER BY source, check_name, created_ts DESC
)
SELECT
  source,
  check_name,
  status,
  total_rows,
  failed_rows,
  details_json,
  created_ts,
  CASE status
    WHEN 'PASS' THEN 1.0
    WHEN 'WARN' THEN 0.5
    WHEN 'FAIL' THEN 0.0
    ELSE 0.0
  END AS score
FROM latest;

CREATE OR REPLACE VIEW dq_source_health AS
SELECT
  source,
  ROUND(100 * AVG(score))::int AS health_score_0_100,
  SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END)::int AS pass_checks,
  SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END)::int AS warn_checks,
  SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END)::int AS fail_checks,
  MAX(created_ts) AS last_run_ts
FROM dq_latest_scored
GROUP BY source;
