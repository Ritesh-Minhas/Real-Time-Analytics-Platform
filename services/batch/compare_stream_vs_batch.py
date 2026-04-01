import time
from sqlalchemy import create_engine, text

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

# Status thresholds (tune for your thesis)
# - FAIL: count diff pct > 5% OR avg diff pct > 5%
# - WARN: count diff pct > 1% OR avg diff pct > 1%
COMPARE_SQL = """
WITH dq AS (
  -- Pull latest DQ status per source for lateness + duplicates (helps root-cause explanations)
  SELECT DISTINCT ON (source, check_name)
    source, check_name, status, details_json, created_ts
  FROM dq_results
  WHERE check_name IN ('late_events_over_30s_rate', 'duplicate_event_id_rate')
  ORDER BY source, check_name, created_ts DESC
),
dq_pivot AS (
  SELECT
    source,
    MAX(CASE WHEN check_name='late_events_over_30s_rate' THEN status END) AS dq_late_status,
    MAX(CASE WHEN check_name='duplicate_event_id_rate' THEN status END) AS dq_dup_status,
    MAX(created_ts) AS dq_last_ts
  FROM dq
  GROUP BY source
),
joined AS (
  SELECT
    COALESCE(g.window_start, b.window_start) AS window_start,
    COALESCE(g.source, b.source) AS source,
    COALESCE(g.key, b.key) AS key,

    g.count AS stream_count,
    b.count AS batch_count,

    g.avg_value AS stream_avg,
    b.avg_value AS batch_avg,

    g.min_value AS stream_min,
    b.min_value AS batch_min,

    g.max_value AS stream_max,
    b.max_value AS batch_max
  FROM gold_minute_aggregates g
  FULL OUTER JOIN batch_minute_aggregates b
    ON g.window_start = b.window_start
   AND g.source = b.source
   AND g.key = b.key
  WHERE COALESCE(g.window_start, b.window_start) >= NOW() - (:hours || ' hours')::interval
),
metrics AS (
  SELECT
    j.*,

    (COALESCE(stream_count,0) - COALESCE(batch_count,0))::bigint AS count_diff,
    CASE
      WHEN COALESCE(batch_count,0) = 0 THEN NULL
      ELSE ((COALESCE(stream_count,0) - COALESCE(batch_count,0))::double precision / batch_count::double precision) * 100
    END AS count_diff_pct,

    (COALESCE(stream_avg,0) - COALESCE(batch_avg,0))::double precision AS avg_diff,
    CASE
      WHEN batch_avg IS NULL OR batch_avg = 0 THEN NULL
      ELSE ((COALESCE(stream_avg,0) - COALESCE(batch_avg,0)) / batch_avg) * 100
    END AS avg_diff_pct,

    (COALESCE(stream_min,0) - COALESCE(batch_min,0))::double precision AS min_diff,
    (COALESCE(stream_max,0) - COALESCE(batch_max,0))::double precision AS max_diff
  FROM joined j
),
scored AS (
  SELECT
    m.*,
    CASE
      WHEN (count_diff_pct IS NOT NULL AND ABS(count_diff_pct) > 5.0)
        OR (avg_diff_pct IS NOT NULL AND ABS(avg_diff_pct) > 5.0) THEN 'FAIL'
      WHEN (count_diff_pct IS NOT NULL AND ABS(count_diff_pct) > 1.0)
        OR (avg_diff_pct IS NOT NULL AND ABS(avg_diff_pct) > 1.0) THEN 'WARN'
      ELSE 'MATCH'
    END AS status
  FROM metrics m
),
tagged AS (
  SELECT
    s.*,
    dqp.dq_late_status,
    dqp.dq_dup_status,
    dqp.dq_last_ts,

    -- Root-cause tags: array of strings (NULLs removed)
    array_remove(ARRAY[
      CASE WHEN s.stream_count IS NULL AND s.batch_count IS NOT NULL THEN 'stream_missing_or_lag' END,
      CASE WHEN s.batch_count IS NULL AND s.stream_count IS NOT NULL THEN 'batch_missing_or_not_run' END,
      CASE WHEN s.batch_count IS NOT NULL AND s.batch_count = 0 AND s.stream_count IS NOT NULL AND s.stream_count > 0 THEN 'batch_zero_possible_filtering' END,

      CASE WHEN (s.count_diff_pct IS NOT NULL AND ABS(s.count_diff_pct) > 1.0)
             AND (dqp.dq_late_status IN ('WARN','FAIL')) THEN 'late_arrivals_high' END,

      CASE WHEN (s.count_diff_pct IS NOT NULL AND ABS(s.count_diff_pct) > 1.0)
             AND (dqp.dq_dup_status IN ('WARN','FAIL')) THEN 'dedupe_mismatch_suspected' END,

      CASE WHEN (s.avg_diff_pct IS NOT NULL AND ABS(s.avg_diff_pct) > 1.0)
             AND (s.count_diff_pct IS NULL OR ABS(s.count_diff_pct) <= 1.0) THEN 'value_distribution_shift' END,

      CASE WHEN s.window_start >= NOW() - INTERVAL '2 minutes' THEN 'near_real_time_window' END
    ], NULL) AS root_cause_tags,

    jsonb_build_object(
      'thresholds', jsonb_build_object('warn_pct', 1.0, 'fail_pct', 5.0),
      'dq_late_status', dqp.dq_late_status,
      'dq_dup_status', dqp.dq_dup_status,
      'dq_last_ts', dqp.dq_last_ts
    ) AS details_json
  FROM scored s
  LEFT JOIN dq_pivot dqp
    ON dqp.source = s.source
)

INSERT INTO stream_batch_comparison
(window_start, source, key,
 stream_count, batch_count, count_diff, count_diff_pct,
 stream_avg, batch_avg, avg_diff, avg_diff_pct,
 stream_min, batch_min, min_diff,
 stream_max, batch_max, max_diff,
 status, compared_ts, root_cause_tags, details_json)
SELECT
  window_start, source, key,
  stream_count, batch_count, count_diff, count_diff_pct,
  stream_avg, batch_avg, avg_diff, avg_diff_pct,
  stream_min, batch_min, min_diff,
  stream_max, batch_max, max_diff,
  status, NOW(), root_cause_tags, details_json
FROM tagged
ON CONFLICT (window_start, source, key)
DO UPDATE SET
  stream_count = EXCLUDED.stream_count,
  batch_count = EXCLUDED.batch_count,
  count_diff = EXCLUDED.count_diff,
  count_diff_pct = EXCLUDED.count_diff_pct,
  stream_avg = EXCLUDED.stream_avg,
  batch_avg = EXCLUDED.batch_avg,
  avg_diff = EXCLUDED.avg_diff,
  avg_diff_pct = EXCLUDED.avg_diff_pct,
  stream_min = EXCLUDED.stream_min,
  batch_min = EXCLUDED.batch_min,
  min_diff = EXCLUDED.min_diff,
  stream_max = EXCLUDED.stream_max,
  batch_max = EXCLUDED.batch_max,
  max_diff = EXCLUDED.max_diff,
  status = EXCLUDED.status,
  compared_ts = NOW(),
  root_cause_tags = EXCLUDED.root_cause_tags,
  details_json = EXCLUDED.details_json;
"""


def main(hours: int = 6, every_seconds: int = 30):
    print(f"[compare_stream_vs_batch] Comparing last {hours} hours every {every_seconds}s")
    while True:
        with ENGINE.begin() as conn:
            conn.execute(text(COMPARE_SQL), {"hours": str(hours)})
        print("[compare_stream_vs_batch] comparison upsert done")
        time.sleep(every_seconds)

if __name__ == "__main__":
    main()
