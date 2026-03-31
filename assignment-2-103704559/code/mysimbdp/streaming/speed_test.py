#!/usr/bin/env python3
"""
speed_test.py  —  Streaming Speed & Window Parameter Test (Part 2 Q3)
======================================================================

WHAT THIS DOES
--------------
Sends bursts of valid sensor records at different speeds to test how
streamanalyticsapp.py behaves when:
  (i)  the speed of streaming data is increased/varied
  (ii) window function parameters are changed (via env vars in docker-compose)

This script controls the PRODUCER side of the speed test.
To change window parameters, update WINDOW_DURATION / WINDOW_SLIDE env vars
in docker-compose.yaml and restart the spark-streaming service.

SPEED LEVELS
------------
  slow   — 1 msg/sec   (baseline)
  medium — 10 msg/sec  (normal load)
  fast   — 100 msg/sec (high throughput)
  flood  — as fast as possible (stress test)

USAGE (run from host machine)
------------------------------
  python speed_test.py --speed slow    --duration 60
  python speed_test.py --speed medium  --duration 60
  python speed_test.py --speed fast    --duration 60
  python speed_test.py --speed flood   --duration 30
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "tenantA.bronze.raw"

COUNTRIES  = ["FI", "DE", "FR", "SE", "NO", "NL", "PL", "IT", "ES", "FI"]
SENSOR_IDS = list(range(10000, 10050))

SPEED_CONFIGS = {
    "slow":   {"delay": 1.0,   "label": "1 msg/sec"},
    "medium": {"delay": 0.1,   "label": "10 msg/sec"},
    "fast":   {"delay": 0.01,  "label": "100 msg/sec"},
    "flood":  {"delay": 0.0,   "label": "max speed"},
}


def make_record(sensor_id: int, country: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "tenant_id":    "tenantA",
        "event_id":     random.randint(1, 9999999),
        "timestamp":    now.strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id":    sensor_id,
        "lat":          round(random.uniform(50.0, 70.0), 4),
        "lon":          round(random.uniform(5.0,  30.0), 4),
        "alt":          round(random.uniform(0.0,  200.0), 1),
        "country":      country,
        "sensor_type":  random.choice(["SDS011", "BME280", "PMS7003"]),
        "pm10_P1":      round(random.uniform(2.0,  80.0), 2),
        "pm2_5_P2":     round(random.uniform(1.0,  60.0), 2),
        "ingest_ts_utc": now.isoformat() + "Z",
        "source":       "speed_test.py",
    }


def delivery_cb(err, msg):
    if err:
        print(f"❌ delivery error: {err}")


def main():
    parser = argparse.ArgumentParser(description="Speed test producer for streamanalyticsapp")
    parser.add_argument("--speed",    default="medium",
                        choices=list(SPEED_CONFIGS.keys()))
    parser.add_argument("--duration", type=int, default=60,
                        help="How many seconds to run the test")
    args = parser.parse_args()

    cfg   = SPEED_CONFIGS[args.speed]
    delay = cfg["delay"]

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "batch.num.messages": 500,
    })

    print(f"==> speed_test.py  speed={args.speed} ({cfg['label']})  duration={args.duration}s")
    print(f"    topic={TOPIC}  kafka={KAFKA_BOOTSTRAP}\n")

    t_start = time.time()
    sent    = 0

    try:
        while time.time() - t_start < args.duration:
            sensor_id = random.choice(SENSOR_IDS)
            country   = random.choice(COUNTRIES)
            record    = make_record(sensor_id, country)

            producer.produce(
                TOPIC,
                key=str(sensor_id).encode(),
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_cb,
            )
            producer.poll(0)
            sent += 1

            if sent % 100 == 0:
                elapsed = time.time() - t_start
                rate    = sent / elapsed if elapsed > 0 else 0
                print(f"  sent={sent}  elapsed={elapsed:.1f}s  rate={rate:.1f} msg/s")

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\n==> Interrupted by user")
    finally:
        producer.flush(10)
        elapsed = time.time() - t_start
        rate    = sent / elapsed if elapsed > 0 else 0
        print(f"\n==> Done.  sent={sent}  elapsed={elapsed:.1f}s  avg_rate={rate:.1f} msg/s")
        print("    Now check Cassandra: SELECT * FROM tenantA_analytics.window_results LIMIT 20;")


if __name__ == "__main__":
    main()
