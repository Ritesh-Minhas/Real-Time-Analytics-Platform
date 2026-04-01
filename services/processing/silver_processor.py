import json
from typing import Any, Dict, Optional, Tuple

import psycopg2
from dateutil import parser as dtparser
from confluent_kafka import Consumer, Producer

from services.common.config import (
    KAFKA_BOOTSTRAP, RAW_TOPIC, DLQ_TOPIC,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
)

INSERT_SILVER_SQL = """
INSERT INTO silver_clean_events (event_id, source, event_ts, key, value, attributes_json)
VALUES (%s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (event_id) DO NOTHING;
"""

INSERT_DLQ_SQL = """
INSERT INTO dlq_events (
  event_id, source, event_ts,
  kafka_topic, kafka_partition, kafka_offset,
  error_type, error_message, payload_json
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
"""

def normalize_event(evt: Dict[str, Any]) -> Optional[Tuple[str, str, Any, str, float, str]]:
    """
    Returns (event_id, source, event_ts, key, value, attributes_json_str) or None if invalid.
    Normalized schema:
      - key: symbol/topic
      - value: numeric metric (stock.price, social.mentions)
      - attributes_json: remaining fields
    """
    event_id = evt.get("event_id")
    source = evt.get("source")
    event_ts_raw = evt.get("event_ts")

    """
    if not event_id or not source or not event_ts_raw:
        return None

    try:
        event_ts = dtparser.isoparse(event_ts_raw)
    except Exception:
        return None
    """
    if not event_id:
        raise ValueError("missing_event_id")
    if not source:
        raise ValueError("missing_source")
    if not event_ts_raw:
        raise ValueError("missing_event_ts")

    event_ts = dtparser.isoparse(event_ts_raw)

    if source == "stock":
        symbol = evt.get("symbol")
        price = evt.get("price")

        """
        volume = evt.get("volume")
        if not symbol or price is None:
            return None
        try:
            price_f = float(price)
            if price_f <= 0:
                return None
        except Exception:
            return None
        """
        if not symbol:
            raise ValueError("missing_symbol")
        if price is None:
            raise ValueError("missing_price")
        price_f = float(price)
        if price_f <= 0 or price_f > 5000:
            raise ValueError("price_out_of_range")

        attrs = {
            "volume": evt.get("volume"),
            "injected_anomaly": evt.get("injected_anomaly", False),
            "injected_anomaly_type": evt.get("injected_anomaly_type")
        }
        return event_id, source, event_ts, symbol, price_f, json.dumps(attrs)

    if source == "social":
        topic = evt.get("topic")
        mentions = evt.get("mentions")

        """
        sentiment = evt.get("sentiment")
        engagement = evt.get("engagement")
        if not topic or mentions is None:
            return None
        try:
            mentions_f = float(mentions)
            if mentions_f < 0:
                return None
        except Exception:
            return None
        """
        if not topic:
            raise ValueError("missing_topic")
        if mentions is None:
            raise ValueError("missing_mentions")

        mentions_f = float(mentions)
        if mentions_f < 0 or mentions_f > 100000:
            raise ValueError("mentions_out_of_range")

        attrs = {
            "sentiment": evt.get("sentiment"),
            "engagement": evt.get("engagement"),
            "injected_anomaly": evt.get("injected_anomaly", False),
            "injected_anomaly_type": evt.get("injected_anomaly_type")
        }
        return event_id, source, event_ts, topic, mentions_f, json.dumps(attrs)

    # Unknown source
    raise ValueError("unknown_source")


def main():
    print("[silver_processor] Connecting to Postgres...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("[silver_processor] Connecting to Kafka...")
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "silver_processor_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })
    c.subscribe([RAW_TOPIC])

    """
    print(f"[silver_processor] Consuming topic={RAW_TOPIC} bootstrap={KAFKA_BOOTSTRAP}")
    inserted = 0
    skipped = 0
    """
    dlq_prod = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    inserted = 0
    dlq_count = 0

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("[silver_processor] Kafka error:", msg.error())
                continue

            payload_raw = None
            evt = None

            try:
                payload_raw = msg.value().decode("utf-8")
                evt = json.loads(payload_raw)

                norm = normalize_event(evt)
                cur.execute(INSERT_SILVER_SQL, norm)
                inserted += 1

            except Exception as e:
                dlq_count += 1
                error_type = type(e).__name__
                error_message = str(e)

                # best-effort parse basics
                event_id = evt.get("event_id") if isinstance(evt, dict) else None
                source = evt.get("source") if isinstance(evt, dict) else None
                event_ts = None
                if isinstance(evt, dict) and evt.get("event_ts"):
                    try:
                        event_ts = dtparser.isoparse(evt.get("event_ts"))
                    except Exception:
                        event_ts = None

                # Write to DLQ table
                try:
                    cur.execute(
                        INSERT_DLQ_SQL,
                        (
                            event_id, source, event_ts,
                            msg.topic(), msg.partition(), msg.offset(),
                            error_type, error_message,
                            payload_raw if payload_raw else json.dumps({"raw": None})
                        )
                    )
                except Exception as db_e:
                    print("[silver_processor] DLQ DB insert failed:", db_e)

                # Also publish to DLQ Kafka topic (optional but nice)
                try:
                    dlq_prod.produce(DLQ_TOPIC, value=(payload_raw or "").encode("utf-8"))
                    dlq_prod.poll(0)
                except Exception as k_e:
                    print("[silver_processor] DLQ Kafka produce failed:", k_e)

            if (inserted + dlq_count) % 200 == 0:
                print(f"[silver_processor] inserted={inserted} dlq={dlq_count}")

            """
            try:
                evt = json.loads(msg.value().decode("utf-8"))
            except Exception:
                skipped += 1
                continue

            norm = normalize_event(evt)
            if not norm:
                skipped += 1
                continue

            cur.execute(INSERT_SILVER_SQL, norm)
            inserted += 1

            if (inserted + skipped) % 200 == 0:
                print(f"[silver_processor] inserted={inserted} skipped={skipped}")
            """

    finally:
        c.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
