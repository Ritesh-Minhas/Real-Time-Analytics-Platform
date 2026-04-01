import json
import time
import uuid
from datetime import datetime, timezone, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

SILVER_SQL = """
SELECT event_id, source, key, event_ts, ingest_ts, value, attributes_json
FROM silver_clean_events
WHERE event_ts >= :window_start
  AND event_ts < :window_end
  AND source = :source
ORDER BY event_ts ASC
"""

ALERTS_SQL = """
SELECT source, key, event_id, event_ts, score, anomaly_type, created_ts, method
FROM alerts_anomalies
WHERE event_ts >= :window_start
  AND event_ts < :window_end
  AND source = :source
"""

INSERT_EVAL_SQL = """
INSERT INTO evaluation_runs
(run_id, window_start, window_end, source, method, throughput_events, eps,
 latency_p50_seconds, latency_p95_seconds, tp, fp, fn,
 precision, recall, f1, details_json)
VALUES
(:run_id, :window_start, :window_end, :source, :method, :throughput_events, :eps,
 :latency_p50_seconds, :latency_p95_seconds, :tp, :fp, :fn,
 :precision, :recall, :f1, CAST(:details_json AS jsonb))
"""

def utc_now():
    return datetime.now(timezone.utc)

def safe_div(a, b):
    return float(a) / float(b) if b else None

def f1_score(p, r):
    if p is None or r is None or (p + r) == 0:
        return None
    return 2 * p * r / (p + r)

def extract_ground_truth(silver: pd.DataFrame) -> set:
    gt = set()

    if silver.empty:
        return gt

    def get_injected(x):
        if isinstance(x, dict):
            return x.get("injected_anomaly", False), x.get("injected_anomaly_type")
        try:
            d = json.loads(x)
            return d.get("injected_anomaly", False), d.get("injected_anomaly_type")
        except Exception:
            return False, None

    tmp = silver[["event_id", "attributes_json"]].copy()
    tmp["injected"] = tmp["attributes_json"].apply(lambda x: get_injected(x)[0])

    gt_df = tmp[tmp["injected"] == True].copy()
    for row in gt_df.itertuples(index=False):
        gt.add(str(row.event_id))

    return gt

def latency_stats(silver: pd.DataFrame):
    if silver.empty:
        return None, None

    silver = silver.copy()
    silver["event_ts"] = pd.to_datetime(silver["event_ts"], utc=True)
    silver["ingest_ts"] = pd.to_datetime(silver["ingest_ts"], utc=True)

    lat = (silver["ingest_ts"] - silver["event_ts"]).dt.total_seconds()
    return float(lat.quantile(0.50)), float(lat.quantile(0.95))

def compute_metrics_for_method(source: str, method: str, silver: pd.DataFrame, alerts: pd.DataFrame,
                               window_start, window_end, minutes: int):
    run_id = str(uuid.uuid4())

    throughput_events = int(len(silver))
    eps = throughput_events / (minutes * 60)

    latency_p50, latency_p95 = latency_stats(silver)
    gt = extract_ground_truth(silver)

    if method == "ALL":
        a = alerts.copy()
    else:
        a = alerts[alerts["method"] == method].copy()

    pred = set(a["event_id"].dropna().astype(str).tolist()) if not a.empty else set()

    tp = len(gt.intersection(pred))
    fp = len(pred - gt)
    fn = len(gt - pred)

    precision = safe_div(tp, tp + fp)
    recall = safe_div(tp, tp + fn)
    f1 = f1_score(precision, recall)

    details = {
        "window_minutes": minutes,
        "match_method": "event_id",
        "gt_injected_count": len(gt),
        "pred_alert_count": len(pred),
        "method": method,
        "notes": "Method-wise Precision/Recall/F1 computed using injected anomaly labels from simulator events."
    }

    return {
        "run_id": run_id,
        "window_start": window_start,
        "window_end": window_end,
        "source": source,
        "method": method,
        "throughput_events": throughput_events,
        "eps": eps,
        "latency_p50_seconds": latency_p50,
        "latency_p95_seconds": latency_p95,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "details_json": json.dumps(details),
    }

def compute_source_metrics(source: str, minutes: int = 10):
    window_end = utc_now()
    window_start = window_end - timedelta(minutes=minutes)

    with ENGINE.connect() as conn:
        silver = pd.read_sql(
            text(SILVER_SQL),
            conn,
            params={"window_start": window_start, "window_end": window_end, "source": source},
        )
        alerts = pd.read_sql(
            text(ALERTS_SQL),
            conn,
            params={"window_start": window_start, "window_end": window_end, "source": source},
        )

    rows = []
    for method in ["EWMA_Z", "IFOR", "ALL"]:
        rows.append(
            compute_metrics_for_method(
                source=source,
                method=method,
                silver=silver,
                alerts=alerts,
                window_start=window_start,
                window_end=window_end,
                minutes=minutes,
            )
        )
    return rows

def main(every_seconds: int = 30, window_minutes: int = 10):
    print(f"[evaluation_runner] Running every {every_seconds}s over last {window_minutes} minutes")
    while True:
        rows = []
        for source in ["stock", "social"]:
            rows.extend(compute_source_metrics(source, minutes=window_minutes))

        with ENGINE.begin() as conn:
            for r in rows:
                conn.execute(text(INSERT_EVAL_SQL), r)

        pretty = {}
        for r in rows:
            pretty[f"{r['source']}-{r['method']}"] = {
                "p": None if r["precision"] is None else round(r["precision"], 3),
                "r": None if r["recall"] is None else round(r["recall"], 3),
                "f1": None if r["f1"] is None else round(r["f1"], 3),
                "alerts": json.loads(r["details_json"])["pred_alert_count"]
            }

        print("[evaluation_runner] wrote metrics:", pretty)
        time.sleep(every_seconds)

if __name__ == "__main__":
    main()