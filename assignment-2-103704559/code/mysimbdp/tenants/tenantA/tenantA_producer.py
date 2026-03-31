#!/usr/bin/env python3
"""
tenantA_producer.py  —  mysimbdp TenantA Kafka Producer
========================================================

WHAT THIS FILE DOES
-------------------
This script runs on the HOST machine (not in Docker).
It fetches air quality sensor data from the sensor.community API
and publishes it to the Kafka topic tenantA.bronze.raw.

TenantA uses a FLAT schema — all fields at the top level of the JSON message.
This is optimized for analytics: the silver pipeline can read fields without
any nested parsing.

DATA FLOW
---------
  sensor.community API
    → fetch one random sensor reading
    → build flat JSON event (sensor_id, lat, lon, pm10_P1, pm2_5_P2, etc.)
    → deduplicate (skip if same sensor_id + timestamp already sent)
    → produce to Kafka topic: tenantA.bronze.raw
    → worker consumes from that topic → inserts to tenantA_bronze.records

KAFKA TOPIC
-----------
  tenantA.bronze.raw
  Partitions: 6 (set in bootstrap.sh)
  Partition key: sensor_id (ensures messages for the same sensor go to same partition)

MESSAGE FORMAT (flat JSON)
--------------------------
  {
    "tenant_id":    "tenantA",
    "event_id":     123456,          ← numeric ID from source API
    "timestamp":    "2026-03-10 23:00:00",
    "sensor_id":    78901,           ← primary key for bronze table
    "lat":          60.17,
    "lon":          24.94,
    "alt":          10.0,
    "country":      "FI",
    "sensor_type":  "SDS011",
    "pm10_P1":      12.3,            ← PM10 concentration μg/m³
    "pm2_5_P2":     5.6,             ← PM2.5 concentration μg/m³
    "raw_values":   {"P1": "12.3", "P2": "5.6", ...},
    "source":       "sensor.community/static/v2/data.json"
  }

DEDUPLICATION
-------------
  A local set (seen) tracks (sensor_id, timestamp) pairs.
  The API serves a rolling snapshot — the same sensor may appear
  in consecutive fetches. We skip duplicates to avoid re-inserting
  the same reading into Cassandra.
  The dedup set is capped at DEDUP_MAX_SIZE and periodically cleared.
"""

import json
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests
from confluent_kafka import Producer


# ── configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")  # overridden in Docker via env var
TOPIC           = "tenantA.bronze.raw"

# Primary API endpoint; fallback if primary is unreachable
URL_PRIMARY  = "https://data.sensor.community/static/v2/data.json"
URL_FALLBACK = "https://api.sensor.community/static/v2/data.json"

HEADERS = {
    "User-Agent": "airq-streaming-platform/0.1 (contact: sabeeh.waqas@aalto.fi)"
}

# How fast to poll — reduce to 0.1s for high-throughput load testing (P1.3)
POLL_INTERVAL_SEC  = float(os.getenv("POLL_INTERVAL_SEC", "0.1"))  # overridden in Docker via env var
FETCH_TIMEOUT_SEC  = 20
MAX_FETCH_ATTEMPTS = 5

# Stop dedup set growing unbounded — clear it after this many unique entries
DEDUP_MAX_SIZE = 5000


# ── API fetch helper ──────────────────────────────────────────────────────────

def fetch_random_object() -> Dict[str, Any]:
    """
    Fetch the full sensor data list and return one random object.
    The API returns ~10,000+ sensor readings per call.
    We pick one randomly to distribute load across different sensors.

    Retries up to MAX_FETCH_ATTEMPTS times with exponential backoff.
    Falls back from primary URL to fallback URL automatically.
    """
    urls     = [URL_PRIMARY, URL_FALLBACK]
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        for url in urls:
            try:
                r = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT_SEC)
                r.raise_for_status()
                data = r.json()
                if not isinstance(data, list) or not data:
                    raise ValueError("Unexpected payload (not a non-empty list)")
                return random.choice(data)
            except (requests.exceptions.RequestException, ValueError) as e:
                last_err = e

        backoff = 2 * attempt
        print(f"⚠️ Fetch failed (attempt {attempt}/{MAX_FETCH_ATTEMPTS}). "
              f"Retrying in {backoff}s... ({last_err})")
        time.sleep(backoff)

    raise RuntimeError(f"Failed to fetch data after retries. Last error: {last_err}")


def sensordatavalues_to_map(obj: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert the sensordatavalues list from the API to a simple dict.
    Input:  [{"value_type": "P1", "value": "12.3"}, {"value_type": "P2", ...}]
    Output: {"P1": "12.3", "P2": "5.6"}
    Values are kept as strings here — safe_float handles conversion later.
    """
    out: Dict[str, str] = {}
    for item in obj.get("sensordatavalues", []) or []:
        vt = item.get("value_type")
        v  = item.get("value")
        if vt is not None and v is not None:
            out[str(vt)] = str(v)
    return out


def safe_float(x: Any) -> Optional[float]:
    try:    return float(x)
    except: return None


# ── TenantA message builder ───────────────────────────────────────────────────

def build_tenantA_event(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a flat TenantA JSON message from one raw API object.

    Design decision: flatten everything to top level for analytics convenience.
    The silver pipeline (silverpipeline_tenantA.py) can read any field with a
    simple payload_obj.get("field_name") call — no nested traversal needed.
    """
    loc        = obj.get("location",    {}) or {}
    sensor     = obj.get("sensor",      {}) or {}
    # sensor_type is a nested dict in the API response
    sensor_type = (sensor.get("sensor_type") or {}) if isinstance(
        sensor.get("sensor_type"), dict) else {}
    vals = sensordatavalues_to_map(obj)

    return {
        "tenant_id":    "tenantA",                       # identifies this message's tenant
        "event_id":     obj.get("id"),                   # numeric API record ID
        "timestamp":    obj.get("timestamp"),            # source timestamp string
        "sampling_rate": obj.get("sampling_rate"),

        # Location fields (flattened from API's location sub-object)
        "location_id":  loc.get("id"),
        "lat":          safe_float(loc.get("latitude")),
        "lon":          safe_float(loc.get("longitude")),
        "alt":          safe_float(loc.get("altitude")),
        "country":      loc.get("country"),
        "indoor":       loc.get("indoor"),

        # Sensor fields (flattened from API's sensor sub-object)
        "sensor_id":    sensor.get("id"),                # PRIMARY KEY in bronze
        "pin":          sensor.get("pin"),
        "sensor_type":  sensor_type.get("name"),         # e.g. "SDS011"
        "sensor_mfg":   sensor_type.get("manufacturer"),

        # Particulate matter measurements (the most important fields for analytics)
        "pm10_P1":      safe_float(vals.get("P1")),      # PM10 μg/m³
        "pm2_5_P2":     safe_float(vals.get("P2")),      # PM2.5 μg/m³

        # Full raw measurement map (for traceability)
        "raw_values":   vals,

        "source":       "sensor.community/static/v2/data.json",
        "ingest_ts_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


# ── Kafka producer ────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    """Kafka delivery callback — called for each produced message."""
    if err is not None:
        print(f"❌ Delivery failed: {err}")

def make_kafka_producer() -> Producer:
    """
    Create an idempotent Kafka producer with sensible defaults.
    enable.idempotence=True + acks=all ensures no duplicates on retry.
    linger.ms=20 batches messages for 20ms to improve throughput.
    """
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "enable.idempotence": True,     # exactly-once delivery
        "acks":               "all",    # wait for all replicas to confirm
        "retries":            5,
        "linger.ms":          20,       # batch messages for 20ms
        "batch.num.messages": 1000,
    })

def choose_partition_key(event: Dict[str, Any]) -> str:
    """
    Kafka partition key — determines which partition the message goes to.
    Using sensor_id ensures all readings from the same sensor go to the same
    partition, preserving ordering per sensor.
    """
    key = event.get("sensor_id") or event.get("location_id") or event.get("event_id")
    return str(key)

def dedupe_key(event: Dict[str, Any]) -> Tuple[Any, Any]:
    """Dedup key: (sensor_id, timestamp). Same reading = same key = skip."""
    return (event.get("sensor_id"), event.get("timestamp"))


# ── main loop ─────────────────────────────────────────────────────────────────

def main():
    p    = make_kafka_producer()
    seen = set()   # dedup set

    print(f"==> TenantA producer started. Kafka={KAFKA_BOOTSTRAP}, topic={TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            obj   = fetch_random_object()
            event = build_tenantA_event(obj)

            # Skip if we've already sent this exact sensor+timestamp reading
            dk = dedupe_key(event)
            if dk in seen:
                time.sleep(0.2)
                continue
            seen.add(dk)
            # Prevent the dedup set from growing forever
            if len(seen) > DEDUP_MAX_SIZE:
                seen.clear()

            key = choose_partition_key(event)
            p.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                callback=delivery_report,
            )
            p.poll(0)   # trigger delivery callbacks without blocking

            print(f"TenantA → topic={TOPIC} key={key} "
                  f"ts={event.get('timestamp')} sensor_id={event.get('sensor_id')}")
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n==> Stopping TenantA producer...")
    finally:
        p.flush(10)   # ensure all buffered messages are delivered before exit
        print("✅ TenantA producer stopped.")


if __name__ == "__main__":
    main()