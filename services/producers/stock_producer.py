import random
import time
from confluent_kafka import Producer

from services.common.config import KAFKA_BOOTSTRAP, RAW_TOPIC
from services.common.utils import now_utc_iso, new_event_id, to_json_bytes

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]

def make_stock_event():
    symbol = random.choice(SYMBOLS)
    base = {
        "AAPL": 185, "MSFT": 410, "GOOGL": 150, "AMZN": 175, "TSLA": 210, "NVDA": 720
    }[symbol]
    price = round(random.gauss(base, base * 0.004), 2)      # small fluctuation
    
    # Injected Anomaly
    injected_anomaly = False
    anomaly_type = None

    # 0.5% injected anomaly for evaluation
    if random.random() < 0.005:
        injected_anomaly = True
        anomaly_type = random.choice(["spike", "drop"])
        price = price * (1.6 if anomaly_type == "spike" else 0.6)
        price = round(price, 2)

    volume = int(abs(random.gauss(5000, 1500)))
    return {
        "event_id": new_event_id(),
        "source": "stock",
        "event_ts": now_utc_iso(),
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "injected_anomaly": injected_anomaly,
        "injected_anomaly_type": anomaly_type
    }

def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"[stock_producer] Publishing to topic={RAW_TOPIC} bootstrap={KAFKA_BOOTSTRAP}")
    while True:
        evt = make_stock_event()
        key = evt["symbol"]
        p.produce(RAW_TOPIC, key=key, value=to_json_bytes(evt))
        p.poll(0)
        time.sleep(0.2)  # ~5 events/sec

if __name__ == "__main__":
    main()
