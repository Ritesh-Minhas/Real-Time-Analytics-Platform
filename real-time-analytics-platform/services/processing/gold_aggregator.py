import time
import psycopg2

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

UPSERT_GOLD_SQL = """
INSERT INTO gold_minute_aggregates
(window_start, source, key, count, avg_value, min_value, max_value, last_updated_ts)
SELECT
  date_trunc('minute', event_ts) AS window_start,
  source,
  key,
  COUNT(*)::bigint AS count,
  AVG(value)::double precision AS avg_value,
  MIN(value)::double precision AS min_value,
  MAX(value)::double precision AS max_value,
  NOW() AS last_updated_ts
FROM silver_clean_events
WHERE event_ts >= NOW() - INTERVAL '120 minutes'
GROUP BY 1, 2, 3
ON CONFLICT (window_start, source, key)
DO UPDATE SET
  count = EXCLUDED.count,
  avg_value = EXCLUDED.avg_value,
  min_value = EXCLUDED.min_value,
  max_value = EXCLUDED.max_value,
  last_updated_ts = NOW();
"""

def main():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("[gold_aggregator] Starting upsert loop (every 10 seconds)...")
    try:
        while True:
            cur.execute(UPSERT_GOLD_SQL)
            print("[gold_aggregator] upserted last 120 minutes into gold")
            time.sleep(10)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
