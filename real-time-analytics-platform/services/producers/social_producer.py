import random
import time
from confluent_kafka import Producer

from services.common.config import KAFKA_BOOTSTRAP, RAW_TOPIC
from services.common.utils import now_utc_iso, new_event_id, to_json_bytes

TOPICS = [
    "AI", "Python", "Cricket", "Bollywood", "Elections", "Crypto",
    "Space", "Climate", "Gaming", "Startups"
]

def make_social_event():
    topic = random.choice(TOPICS)
    # sentiment: -1 to +1
    sentiment = max(-1.0, min(1.0, random.gauss(0.1, 0.5)))
    mentions = int(abs(random.gauss(200, 80)))

    # Injected Anomaly
    injected_anomaly = False
    anomaly_type = None

    # 1% injected spike in mentions
    if random.random() < 0.01:
        injected_anomaly = True
        anomaly_type = "spike"
        mentions = mentions * 15

    engagement = int(abs(random.gauss(800, 250)))
    return {
        "event_id": new_event_id(),
        "source": "social",
        "event_ts": now_utc_iso(),
        "topic": topic,
        "sentiment": round(sentiment, 3),
        "mentions": mentions,
        "engagement": engagement,
        "injected_anomaly": injected_anomaly,
        "injected_anomaly_type": anomaly_type
    }

def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"[social_producer] Publishing to topic={RAW_TOPIC} bootstrap={KAFKA_BOOTSTRAP}")
    while True:
        evt = make_social_event()
        key = evt["topic"]
        p.produce(RAW_TOPIC, key=key, value=to_json_bytes(evt))
        p.poll(0)
        time.sleep(0.3)  # ~3-4 events/sec

if __name__ == "__main__":
    main()
