#!/usr/bin/env python3
"""
alert_consumer.py  —  TenantA Alert Consumer (simulates tenant service)
========================================================================

Consumes from Kafka topic 'tenantA.alerts' and prints each alert.
Includes retry logic so it waits for Kafka to be ready on startup
instead of crashing immediately.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
ALERT_TOPIC     = os.getenv("KAFKA_ALERT_TOPIC", "tenantA.alerts")
GROUP_ID        = os.getenv("CONSUMER_GROUP", "tenantA-alert-service")
RETRY_SECONDS   = 5
MAX_RETRIES     = 24   # wait up to 2 minutes for Kafka


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def wait_for_kafka():
    """Retry connecting to Kafka until it's ready. Exits after MAX_RETRIES."""
    from confluent_kafka.admin import AdminClient
    print(f"==> Waiting for Kafka at {KAFKA_BOOTSTRAP} ...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP,
                                 "socket.timeout.ms": 3000})
            meta = admin.list_topics(timeout=5)
            print(f"✅ Kafka ready — topics visible: {len(meta.topics)}")
            return
        except Exception as e:
            print(f"  [{attempt}/{MAX_RETRIES}] Kafka not ready yet: {e} — retrying in {RETRY_SECONDS}s")
            time.sleep(RETRY_SECONDS)
    print("❌ Kafka never became ready. Exiting.")
    sys.exit(1)


def main():
    wait_for_kafka()

    consumer = Consumer({
        "bootstrap.servers":       KAFKA_BOOTSTRAP,
        "group.id":                GROUP_ID,
        "auto.offset.reset":       "latest",
        "enable.auto.commit":      True,
        # Don't fail hard if topic doesn't exist yet — wait for it
        "allow.auto.create.topics": False,
    })

    consumer.subscribe([ALERT_TOPIC])
    print(f"==> TenantA Alert Consumer started  ts={now_iso()}")
    print(f"    Listening on {KAFKA_BOOTSTRAP} / {ALERT_TOPIC}")
    print("    Waiting for alerts... (Ctrl+C to stop)\n")

    running = True

    def _stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            # UNKNOWN_TOPIC means alerts topic not created yet — just wait
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"⏳ Alert topic '{ALERT_TOPIC}' not ready yet — waiting...")
                time.sleep(5)
                continue
            print(f"❌ Kafka error: {msg.error()}", file=sys.stderr)
            continue

        try:
            alert        = json.loads(msg.value().decode("utf-8"))
            country      = alert.get("country", "?")
            avg_pm25     = alert.get("avg_pm25", 0)
            avg_pm10     = alert.get("avg_pm10", 0)
            alert_reason = alert.get("alert_reason", "?")
            window_start = alert.get("window_start", "?")

            print(
                f"🚨 ALERT RECEIVED  country={country}  "
                f"PM2.5={avg_pm25:.2f}μg/m³  PM10={avg_pm10:.2f}μg/m³  "
                f"reason={alert_reason}  window={window_start}"
            )
        except Exception as e:
            print(f"⚠️ Could not parse alert message: {e}  raw={msg.value()}")

    consumer.close()
    print("\n==> TenantA Alert Consumer stopped.")


if __name__ == "__main__":
    main()