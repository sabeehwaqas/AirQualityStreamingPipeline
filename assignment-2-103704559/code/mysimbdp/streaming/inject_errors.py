#!/usr/bin/env python3
"""
inject_errors.py  —  Erroneous Record Injector for Part 2 Q4 Testing
======================================================================

WHAT THIS DOES
--------------
Injects deliberately malformed messages into the Kafka topic 'tenantA.bronze.raw'
to test how streamanalyticsapp.py handles bad data.

ERROR TYPES TESTED
------------------
1. MISSING_COUNTRY     — country field is null/absent  (filtered in step 3)
2. MISSING_TIMESTAMP   — timestamp field is null       (filtered in step 3)
3. NEGATIVE_PM         — pm2_5_P2 = -999.0            (passes filter, skews avg)
4. WRONG_TYPES         — pm2_5_P2 = "not-a-number"    (DoubleType parse → null)
5. INVALID_JSON        — completely broken JSON bytes  (from_json → all nulls → dropped)
6. EMPTY_PAYLOAD       — empty string                  (from_json → all nulls → dropped)
7. VALID               — normal record (for baseline comparison)

USAGE
-----
  # Inject 10 of each error type:
  python inject_errors.py --errors-per-type 10

  # Inject only a specific type:
  python inject_errors.py --type MISSING_COUNTRY --errors-per-type 5

  # Fast flood: inject 500 errors total as fast as possible
  python inject_errors.py --errors-per-type 100 --no-delay
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"   # external port (run from host)
TOPIC           = "tenantA.bronze.raw"

VALID_TEMPLATE = {
    "tenant_id":    "tenantA",
    "event_id":     999000,
    "timestamp":    "2026-03-10 12:00:00",
    "sensor_id":    88888,
    "lat":          60.17,
    "lon":          24.94,
    "alt":          10.0,
    "country":      "FI",
    "sensor_type":  "SDS011",
    "pm10_P1":      8.5,
    "pm2_5_P2":     4.2,
    "ingest_ts_utc": datetime.now(timezone.utc).isoformat() + "Z",
    "source":       "inject_errors.py",
}

ERROR_GENERATORS = {
    "MISSING_COUNTRY": lambda: {
        **VALID_TEMPLATE, "country": None, "event_id": random.randint(1, 99999)
    },
    "MISSING_TIMESTAMP": lambda: {
        **VALID_TEMPLATE, "timestamp": None, "event_id": random.randint(1, 99999)
    },
    "NEGATIVE_PM": lambda: {
        **VALID_TEMPLATE,
        "pm2_5_P2": -999.0,    # negative — passes filter but skews analytics
        "pm10_P1":  -999.0,
        "event_id": random.randint(1, 99999)
    },
    "WRONG_TYPES": lambda: {
        **VALID_TEMPLATE,
        "pm2_5_P2": "not-a-number",   # string where DoubleType expected
        "pm10_P1":  "bad-value",
        "event_id": random.randint(1, 99999)
    },
    "INVALID_JSON": lambda: b"{ this is not json !! [[[",     # raw bytes
    "EMPTY_PAYLOAD": lambda: b"",
    "VALID": lambda: {
        **VALID_TEMPLATE, "event_id": random.randint(1, 99999)
    },
}


def delivery_cb(err, msg):
    if err:
        print(f"  ❌ delivery failed: {err}")


def main():
    parser = argparse.ArgumentParser(description="Inject erroneous Kafka records")
    parser.add_argument("--errors-per-type", type=int, default=10,
                        help="Number of messages to inject per error type")
    parser.add_argument("--type", default=None,
                        choices=list(ERROR_GENERATORS.keys()),
                        help="Only inject this error type (default: all types)")
    parser.add_argument("--no-delay", action="store_true",
                        help="Inject as fast as possible (no sleep between messages)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
    })

    types_to_inject = [args.type] if args.type else list(ERROR_GENERATORS.keys())

    print(f"==> inject_errors.py  topic={TOPIC}  errors_per_type={args.errors_per_type}")
    print(f"    Injecting types: {types_to_inject}\n")

    total = 0
    for err_type in types_to_inject:
        gen = ERROR_GENERATORS[err_type]
        print(f"--- Injecting {args.errors_per_type}x {err_type} ---")

        for i in range(args.errors_per_type):
            payload = gen()

            if isinstance(payload, bytes):
                value = payload          # already raw bytes (INVALID_JSON, EMPTY)
            else:
                value = json.dumps(payload, default=str).encode("utf-8")

            producer.produce(
                TOPIC,
                key=b"inject_errors",
                value=value,
                callback=delivery_cb,
            )
            producer.poll(0)
            total += 1

            print(f"  [{err_type}] #{i+1} sent  len={len(value)}B")
            if not args.no_delay:
                time.sleep(0.05)

        print()

    producer.flush(10)
    print(f"==> Done. Total messages injected: {total}")
    print("    Check streamanalyticsapp logs for filter/drop counts.")


if __name__ == "__main__":
    main()
