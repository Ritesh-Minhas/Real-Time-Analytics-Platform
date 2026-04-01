import json
import time
from sqlalchemy import create_engine, text

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

INSERT_METRIC = """
INSERT INTO pipeline_metrics (metric_name, metric_value, labels_json)
VALUES (:name, :value, CAST(:labels AS jsonb))
"""

def write_metric(name: str, value: float, labels: dict):
    with ENGINE.begin() as conn:
        conn.execute(text(INSERT_METRIC), {
            "name": name,
            "value": float(value),
            "labels": json.dumps(labels)
        })

def main(every_seconds: int = 15):
    print(f"[metrics_job] writing metrics every {every_seconds}s")
    while True:
        with ENGINE.connect() as conn:
            bronze_5m = conn.execute(text(
                "SELECT COUNT(*) FROM bronze_raw_events WHERE ingest_ts >= NOW() - INTERVAL '5 minutes'"
            )).scalar()

            silver_5m = conn.execute(text(
                "SELECT COUNT(*) FROM silver_clean_events WHERE ingest_ts >= NOW() - INTERVAL '5 minutes'"
            )).scalar()

            dlq_5m = conn.execute(text(
                "SELECT COUNT(*) FROM dlq_events WHERE created_ts >= NOW() - INTERVAL '5 minutes'"
            )).scalar()

            alerts_60m = conn.execute(text(
                "SELECT COUNT(*) FROM alerts_anomalies WHERE created_ts >= NOW() - INTERVAL '60 minutes'"
            )).scalar()

        write_metric("events_bronze_5m", bronze_5m, {"window": "5m"})
        write_metric("events_silver_5m", silver_5m, {"window": "5m"})
        write_metric("dlq_events_5m", dlq_5m, {"window": "5m"})
        write_metric("alerts_60m", alerts_60m, {"window": "60m"})

        print(f"[metrics_job] bronze_5m={bronze_5m} silver_5m={silver_5m} dlq_5m={dlq_5m} alerts_60m={alerts_60m}")
        time.sleep(every_seconds)

if __name__ == "__main__":
    main()
