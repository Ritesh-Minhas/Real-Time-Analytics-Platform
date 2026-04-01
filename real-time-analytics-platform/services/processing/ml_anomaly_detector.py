import json
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

FETCH_SILVER_SQL = """
SELECT event_id, source, event_ts, key, value
FROM silver_clean_events
WHERE event_ts >= NOW() - INTERVAL '45 minutes'
  AND source = :source
ORDER BY event_ts ASC
"""

RECENT_ALERT_EXISTS_SQL = """
SELECT 1
FROM alerts_anomalies
WHERE source = :source
  AND key = :key
  AND method = :method
  AND metric_name = :metric_name
  AND event_ts >= :event_ts - INTERVAL '2 minutes'
LIMIT 1
"""

INSERT_ALERT_SQL = """
INSERT INTO alerts_anomalies
(alert_id, source, key, event_id, event_ts, window_start, window_end,
 metric_name, actual_value, expected_value, score, severity_score, anomaly_type, details_json,
 method, model_version)
VALUES
(:alert_id, :source, :key, :event_id, :event_ts, :window_start, :window_end,
 :metric_name, :actual_value, :expected_value, :score, :severity_score, :anomaly_type, CAST(:details_json AS jsonb),
 :method, :model_version)
ON CONFLICT (alert_id) DO NOTHING
"""


def utc_now():
    return datetime.now(timezone.utc)


def make_features(kdf: pd.DataFrame) -> pd.DataFrame:
    """Generate time-series features"""
    s = kdf["value"].astype(float)

    feats = pd.DataFrame(index=kdf.index)
    feats["value"] = s
    feats["delta"] = s.diff().fillna(0.0)
    feats["abs_delta"] = feats["delta"].abs()
    feats["roll_mean_10"] = s.rolling(10, min_periods=5).mean()
    feats["roll_std_10"] = s.rolling(10, min_periods=5).std().fillna(0.0)
    feats["roll_min_10"] = s.rolling(10, min_periods=5).min()
    feats["roll_max_10"] = s.rolling(10, min_periods=5).max()

    feats["z_approx"] = (s - feats["roll_mean_10"]) / (
        feats["roll_std_10"].replace(0, np.nan)
    )

    feats["z_approx"] = feats["z_approx"].replace(
        [np.inf, -np.inf], np.nan
    ).fillna(0.0)

    return feats.fillna(0.0)


# ------------------------------------------------
# Root Cause Explanation Generator
# ------------------------------------------------
def generate_explanation(features_row):
    reasons = []

    if abs(features_row["delta"]) > 3 * features_row["roll_std_10"]:
        reasons.append("Sudden value change detected")

    if abs(features_row["z_approx"]) > 3:
        reasons.append("Value deviates strongly from rolling mean")

    if features_row["roll_std_10"] > abs(features_row["value"]) * 0.05:
        reasons.append("Volatility spike detected")

    return reasons


def detect_source(source: str, metric_name: str, contamination: float = 0.01):

    method = "IFOR"
    model_version = "v2"

    with ENGINE.connect() as conn:
        df = pd.read_sql(text(FETCH_SILVER_SQL), conn, params={"source": source})

    if df.empty:
        print(f"[ml_anomaly_detector] source={source} no data")
        return

    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)

    alerts = []

    for key, kdf in df.groupby("key", sort=False):

        kdf = kdf.sort_values("event_ts").reset_index(drop=True)

        if len(kdf) < 80:
            continue

        X = make_features(kdf)

        train = X.iloc[:-20]
        test = X.iloc[-20:]

        clf = IsolationForest(
            n_estimators=150,
            contamination=contamination,
            random_state=42
        )

        clf.fit(train)

        scores = clf.decision_function(test)
        anomaly_score = -scores

        thresh = np.percentile(anomaly_score, 98)

        for idx, (i, row) in enumerate(test.iterrows()):

            s = float(anomaly_score[idx])

            if s < thresh:
                continue

            evt = kdf.iloc[i]
            event_ts = evt["event_ts"].to_pydatetime()

            # Dedup check
            with ENGINE.connect() as conn:
                exists = conn.execute(
                    text(RECENT_ALERT_EXISTS_SQL),
                    {
                        "source": source,
                        "key": key,
                        "method": method,
                        "metric_name": metric_name,
                        "event_ts": event_ts,
                    },
                ).fetchone()

            if exists:
                continue

            severity = max(0.0, min(1.0, s * 2))

            explanation = generate_explanation(X.loc[i])

            alert_id = str(uuid.uuid4())

            window_end = event_ts
            window_start = (evt["event_ts"] - pd.Timedelta(minutes=5)).to_pydatetime()

            expected = float(X.loc[i, "roll_mean_10"])

            anomaly_type = "spike" if float(X.loc[i, "delta"]) > 0 else "drop"

            details = {
                "method": "IsolationForest",
                "contamination": contamination,
                "threshold_percentile": 98,
                "threshold_value": float(thresh),
                "feature_preview": {
                    "delta": float(X.loc[i, "delta"]),
                    "roll_std_10": float(X.loc[i, "roll_std_10"]),
                    "z_approx": float(X.loc[i, "z_approx"])
                },
                "explanation": explanation,
                "severity_score": severity
            }

            alerts.append({
                "alert_id": alert_id,
                "source": source,
                "key": key,
                "event_id": str(evt["event_id"]),
                "event_ts": event_ts,
                "window_start": window_start,
                "window_end": window_end,
                "metric_name": metric_name,
                "actual_value": float(evt["value"]),
                "expected_value": expected,
                "score": s,
                "severity_score": severity,
                "anomaly_type": anomaly_type,
                "details_json": json.dumps(details),
                "method": method,
                "model_version": model_version
            })

    if alerts:
        with ENGINE.begin() as conn:
            conn.execute(text(INSERT_ALERT_SQL), alerts)

        print(f"[ml_anomaly_detector] source={source} inserted={len(alerts)}")
    else:
        print(f"[ml_anomaly_detector] source={source} inserted=0")


def main(every_seconds: int = 15):
    print("[ml_anomaly_detector] Starting IsolationForest detector (every 15s)")

    while True:

        detect_source("stock", metric_name="price", contamination=0.01)
        detect_source("social", metric_name="mentions", contamination=0.02)
        time.sleep(every_seconds)


if __name__ == "__main__":
    main()