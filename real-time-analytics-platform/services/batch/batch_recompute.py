import time
from sqlalchemy import create_engine, text

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

# Recompute batch aggregates for last N hours (stable + fast enough)
UPSERT_BATCH_SQL = """
INSERT INTO batch_minute_aggregates
(window_start, source, key, count, avg_value, min_value, max_value, computed_ts)
SELECT
  date_trunc('minute', event_ts) AS window_start,
  source,
  key,
  COUNT(*)::bigint AS count,
  AVG(value)::double precision AS avg_value,
  MIN(value)::double precision AS min_value,
  MAX(value)::double precision AS max_value,
  NOW() AS computed_ts
FROM silver_clean_events
WHERE event_ts >= NOW() - (:hours || ' hours')::interval
GROUP BY 1, 2, 3
ON CONFLICT (window_start, source, key)
DO UPDATE SET
  count = EXCLUDED.count,
  avg_value = EXCLUDED.avg_value,
  min_value = EXCLUDED.min_value,
  max_value = EXCLUDED.max_value,
  computed_ts = NOW();
"""

def main(hours: int = 6, every_seconds: int = 60):
    print(f"[batch_recompute] Recomputing batch aggregates for last {hours} hours every {every_seconds}s")
    while True:
        with ENGINE.begin() as conn:
            conn.execute(text(UPSERT_BATCH_SQL), {"hours": str(hours)})
        print("[batch_recompute] upsert done")
        time.sleep(every_seconds)

if __name__ == "__main__":
    main()
