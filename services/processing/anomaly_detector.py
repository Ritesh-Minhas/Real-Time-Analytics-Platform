import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

# Pull last 30 minutes; tune as needed
FETCH_SILVER_SQL = """
SELECT event_id, source, event_ts, key, value
FROM silver_clean_events
WHERE event_ts >= NOW() - INTERVAL '30 minutes'
  AND source = :source
ORDER BY event_ts ASC
"""

# Prevent re-alerting too frequently on same key: store alerts only if not present recently
RECENT_ALERT_EXISTS_SQL = """
SELECT 1
FROM alerts_anomalies
WHERE source = :source
  AND key = :key
  AND metric_name = :metric_name
  AND event_ts >= :event_ts - INTERVAL '2 minutes'
LIMIT 1
"""

INSERT_ALERT_SQL = """
INSERT INTO alerts_anomalies
(alert_id, source, key, event_id, event_ts, window_start, window_end,
 metric_name, actual_value, expected_value, score, anomaly_type, details_json)
VALUES
(:alert_id, :source, :key, :event_id, :event_ts, :window_start, :window_end,
 :metric_name, :actual_value, :expected_value, :score, :anomaly_type, :details_json::jsonb)
ON CONFLICT (alert_id) DO NOTHING
"""

def utc_now():
    return datetime.now(timezone.utc)

def compute_ewma_zscores(series: pd.Series, span: int = 20) -> pd.DataFrame:
    """
    EWMA mean and EWMA std approximation using EWMA of squared deviations.
    Returns df with columns: mean, std, z
    """
    s = series.astype(float)
    mean = s.ewm(span=span, adjust=False).mean()
    # EWMA variance proxy
    var = ((s - mean) ** 2).ewm(span=span, adjust=False).mean()
    std = (var ** 0.5).replace(0, pd.NA)
    z = (s - mean) / std
    return pd.DataFrame({"mean": mean, "std": std, "z": z})

def detect_for_source(source: str, metric_name: str, z_thresh: float):
    with ENGINE.connect() as conn:
        df = pd.read_sql(text(FETCH_SILVER_SQL), conn, params={"source": source})

    if df.empty:
        print(f"[anomaly_detector] source={source} no data")
        return

    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)

    alerts_to_write = []
    for key, kdf in df.groupby("key", sort=False):
        # Need enough points
        if len(kdf) < 30:
            continue

        zdf = compute_ewma_zscores(kdf["value"], span=20)
        kdf = kdf.reset_index(drop=True)
        kdf = pd.concat([kdf, zdf], axis=1)

        # Evaluate only last N points to reduce repeated historical checks
        tail = kdf.tail(20)

        for row in tail.itertuples(index=False):
            if pd.isna(row.z) or pd.isna(row.std):
                continue

            z = float(row.z)
            if abs(z) < z_thresh:
                continue

            anomaly_type = "spike" if z > 0 else "drop"

            # Dedupe: don't insert if alert exists within last 2 minutes for same key/metric
            with ENGINE.connect() as conn:
                exists = conn.execute(
                    text(RECENT_ALERT_EXISTS_SQL),
                    {
                        "source": source,
                        "key": key,
                        "metric_name": metric_name,
                        "event_ts": row.event_ts.to_pydatetime(),
                    },
                ).fetchone()
            if exists:
                continue

            alert_id = str(uuid.uuid4())
            window_end = row.event_ts.to_pydatetime()
            window_start = (row.event_ts - pd.Timedelta(minutes=5)).to_pydatetime()

            details = {
                "method": "EWMA_Z",
                "z_threshold": z_thresh,
                "span": 20,
                "std": float(row.std) if row.std is not None else None
            }

            alerts_to_write.append({
                "alert_id": alert_id,
                "source": source,
                "key": key,
                "event_id": row.event_id,
                "event_ts": row.event_ts.to_pydatetime(),
                "window_start": window_start,
                "window_end": window_end,
                "metric_name": metric_name,
                "actual_value": float(row.value),
                "expected_value": float(row.mean) if row.mean is not None else None,
                "score": abs(z),
                "anomaly_type": anomaly_type,
                "details_json": json.dumps(details),
            })

    if alerts_to_write:
        with ENGINE.begin() as conn:
            for a in alerts_to_write:
                conn.execute(text(INSERT_ALERT_SQL), a)
        print(f"[anomaly_detector] source={source} inserted_alerts={len(alerts_to_write)}")
    else:
        print(f"[anomaly_detector] source={source} inserted_alerts=0")

def main(every_seconds: int = 10):
    print("[anomaly_detector] Starting... (every 10s)")
    print("  stock metric=price z>=3.5, social metric=mentions z>=3.5")
    while True:
        detect_for_source("stock", metric_name="price", z_thresh=3.5)
        detect_for_source("social", metric_name="mentions", z_thresh=3.5)
        time.sleep(every_seconds)

if __name__ == "__main__":
    main()
