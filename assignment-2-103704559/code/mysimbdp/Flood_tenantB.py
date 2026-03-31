#!/usr/bin/env python3
"""
flood_tenantB.py  —  P1.3 Under-provisioning load test
=======================================================
Blasts a large number of pre-built messages into tenantB.bronze.raw
as fast as possible, WITHOUT waiting for the sensor.community API.

This fills the Kafka topic with a backlog so the throttled worker
(mem_limit=128m, cpu_quota=25000) genuinely can't keep up,
pushing avg_ingest_ms above the 50ms monitor threshold.

Usage:
    python3 flood_tenantB.py                  # 5000 messages (default)
    python3 flood_tenantB.py --count 20000    # 20k messages
    python3 flood_tenantB.py --count 20000 --kafka localhost:9092
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ── config ────────────────────────────────────────────────────────────────────
TOPIC = "tenantB.bronze.raw"

SAMPLE_COUNTRIES = ["FI", "DE", "SE", "NO", "FR", "NL", "PL", "IT"]
SAMPLE_SENSOR_TYPES = ["SDS011", "BME280", "PMS5003", "SPS30"]


def make_message(i: int) -> dict:
    """Build a realistic-looking TenantB nested message without API calls."""
    location_id = random.randint(10000, 99999)
    sensor_id   = random.randint(1000, 9999)
    ts_str      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "tenant_id": "tenantB",
        "meta": {
            "event_id":      i,
            "timestamp_str": ts_str,
            "ts_epoch":      int(time.time()),
            "source": {
                "provider": "flood_test",
                "endpoint": "synthetic",
            },
            "ingest_ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
        },
        "geo": {
            "location_id": location_id,
            "lat":  round(random.uniform(55.0, 70.0), 4),
            "lon":  round(random.uniform(20.0, 35.0), 4),
            "alt":  round(random.uniform(0.0, 200.0), 1),
            "country":  random.choice(SAMPLE_COUNTRIES),
            "indoor":   0,
            "exact_location": 0,
        },
        "device": {
            "sensor_id": sensor_id,
            "pin": "1",
            "type": {
                "name":         random.choice(SAMPLE_SENSOR_TYPES),
                "manufacturer": "Test",
            },
        },
        "measurements": {
            "P1": str(round(random.uniform(1.0, 80.0), 2)),   # PM10
            "P2": str(round(random.uniform(0.5, 40.0), 2)),   # PM2.5
        },
        "tags": ["bronze", "telemetry", "flood_test"],
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count",  type=int, default=5000,
                        help="Number of messages to produce (default: 5000)")
    parser.add_argument("--kafka",  default="localhost:9092",
                        help="Kafka bootstrap server (default: localhost:9092)")
    args = parser.parse_args()

    p = Producer({
        "bootstrap.servers":  args.kafka,
        "enable.idempotence": True,
        "acks":               "all",
        "linger.ms":          5,
        "batch.num.messages": 5000,
        "queue.buffering.max.messages": 100000,
    })

    print(f"==> Flooding {TOPIC} with {args.count} messages on {args.kafka}...")
    start = time.time()

    for i in range(args.count):
        msg   = make_message(i)
        key   = str(msg["geo"]["location_id"])
        value = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        p.produce(TOPIC, key=key.encode(), value=value, callback=delivery_report)

        # Poll every 1000 messages to drain delivery callbacks
        if i % 1000 == 0:
            p.poll(0)
            elapsed = time.time() - start
            rate    = i / elapsed if elapsed > 0 else 0
            print(f"  Produced {i:>6}/{args.count}  ({rate:.0f} msg/s)")

    print("==> Flushing remaining messages...")
    p.flush(30)

    elapsed = time.time() - start
    print(f"\n✅ Done. {args.count} messages in {elapsed:.1f}s "
          f"({args.count/elapsed:.0f} msg/s)")
    print(f"\nNow watch the throttled worker struggle:")
    print(f"  docker logs -f streamingestworker-tenantB")
    print(f"  curl http://localhost:8001/monitor/alerts")


if __name__ == "__main__":
    main()