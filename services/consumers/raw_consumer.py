import json
from dateutil import parser as dtparser
import psycopg2
from confluent_kafka import Consumer

from services.common.config import (
    KAFKA_BOOTSTRAP, RAW_TOPIC,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
)

INSERT_SQL = """
INSERT INTO bronze_raw_events (event_id, source, event_ts, payload_json)
VALUES (%s, %s, %s, %s::jsonb)
ON CONFLICT DO NOTHING;
"""

def main():
    print("[raw_consumer] Connecting to Postgres...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("[raw_consumer] Connecting to Kafka...")
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "raw_consumer_group",
        "auto.offset.reset": "earliest"
    })
    c.subscribe([RAW_TOPIC])

    print(f"[raw_consumer] Consuming topic={RAW_TOPIC} bootstrap={KAFKA_BOOTSTRAP}")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("[raw_consumer] Kafka error:", msg.error())
                continue

            payload = msg.value().decode("utf-8")
            evt = json.loads(payload)

            event_id = evt.get("event_id")
            source = evt.get("source")
            event_ts = dtparser.isoparse(evt.get("event_ts"))

            cur.execute(
                INSERT_SQL,
                (event_id, source, event_ts, payload)
            )

            print(f"[raw_consumer] inserted event_id={event_id} source={source} key={msg.key()}")
    finally:
        c.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
