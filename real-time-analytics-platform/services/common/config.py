import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw_events")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "rt_analytics")
PG_USER = os.getenv("PG_USER", "rt_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rt_pass")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq_events")
