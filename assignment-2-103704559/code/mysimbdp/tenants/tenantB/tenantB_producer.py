#!/usr/bin/env python3
"""
tenantB_producer.py  —  mysimbdp TenantB Kafka Producer
========================================================

WHAT THIS FILE DOES
-------------------
Same job as tenantA_producer.py but for TenantB.
Uses the SAME sensor.community API but builds a NESTED message structure.

TenantB uses a nested schema with sub-objects for geo, device, meta, and measurements.
This preserves the original structure of the source data, making provenance
easier to trace for location-focused analysis.

MESSAGE FORMAT (nested JSON)
----------------------------
  {
    "tenant_id": "tenantB",
    "meta": {
      "event_id":      456,
      "timestamp_str": "2026-03-10 23:00:00",
      "ts_epoch":      1741654800,
      "source":        {"provider": "sensor.community", ...},
      "ingest_ts_utc": "2026-03-10T23:00:00Z"
    },
    "geo": {
      "location_id": 789,          ← PRIMARY KEY for bronze table
      "lat":         60.17,
      "lon":         24.94,
      "alt":         10.0,
      "country":     "FI",
      "indoor":      0
    },
    "device": {
      "sensor_id": 123,
      "pin":       "1",
      "type":      {"name": "SDS011", "manufacturer": "Nova Fitness"}
    },
    "measurements": {
      "P1": "12.3",    ← PM10
      "P2": "5.6"      ← PM2.5
    },
    "tags": ["bronze", "telemetry"]
  }

KEY DIFFERENCES FROM TENANTA
------------------------------
  - Kafka topic: tenantB.bronze.raw (dedicated topic)
  - Partition key: location_id (not sensor_id)
  - Bronze PK: location_id (multiple sensors can share a location)
  - Schema: nested sub-objects (geo, device, meta, measurements)
  - Dedup key: (location_id, timestamp_str)
"""

import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from confluent_kafka import Producer


# ── configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = "localhost:9092"   # external port (host machine)
TOPIC              = "tenantB.bronze.raw"

URL_PRIMARY  = "https://data.sensor.community/static/v2/data.json"
URL_FALLBACK = "https://api.sensor.community/static/v2/data.json"

HEADERS = {
    "User-Agent": "airq-streaming-platform/0.1 (contact: sabeeh.waqas@aalto.fi)"
}

POLL_INTERVAL_SEC  = 0.1   # lower for intensive load testing
FETCH_TIMEOUT_SEC  = 20
MAX_FETCH_ATTEMPTS = 5
DEDUP_MAX_SIZE     = 5000


# ── API fetch helper ──────────────────────────────────────────────────────────

def fetch_random_object() -> Dict[str, Any]:
    """Fetch one random sensor reading from the API (same as tenantA_producer)."""
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
    """Convert sensordatavalues list → dict. Same as tenantA_producer."""
    out: Dict[str, str] = {}
    for item in obj.get("sensordatavalues", []) or []:
        vt = item.get("value_type")
        v  = item.get("value")
        if vt is not None and v is not None:
            out[str(vt)] = str(v)
    return out


def to_epoch_utc(ts_str: Optional[str]) -> Optional[int]:
    """
    Convert API timestamp string "2026-02-26 22:05:03" to Unix epoch (seconds).
    Stored in meta.ts_epoch for consumers that prefer numeric timestamps.
    """
    if not ts_str:
        return None
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


# ── TenantB message builder ───────────────────────────────────────────────────

def build_tenantB_event(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a nested TenantB JSON message from one raw API object.

    Design decision: preserve the hierarchical structure of the source data.
    The silver pipeline (silverpipeline_tenantB.py) navigates these sub-objects
    to extract fields.

    Sub-object breakdown:
      meta     — platform metadata (event_id, timestamps, source info)
      geo      — location data (location_id is the Cassandra PK)
      device   — sensor hardware info
      measurements — air quality readings (P1=PM10, P2=PM2.5)
    """
    loc    = obj.get("location", {}) or {}
    sensor = obj.get("sensor",   {}) or {}
    stype  = (sensor.get("sensor_type") or {}) if isinstance(
        sensor.get("sensor_type"), dict) else {}
    vals   = sensordatavalues_to_map(obj)

    return {
        "tenant_id": "tenantB",

        # Platform metadata envelope
        "meta": {
            "event_id":      obj.get("id"),                # numeric API ID
            "sampling_rate": obj.get("sampling_rate"),
            "timestamp_str": obj.get("timestamp"),         # source timestamp string
            "ts_epoch":      to_epoch_utc(obj.get("timestamp")),  # numeric epoch
            "source": {
                "provider": "sensor.community",
                "endpoint": "static/v2/data.json",
            },
            "ingest_ts_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },

        # Geographic location sub-object
        "geo": {
            "location_id":    loc.get("id"),               # PRIMARY KEY in bronze
            "lat":            loc.get("latitude"),         # kept as string here
            "lon":            loc.get("longitude"),        # silver pipeline casts to float
            "alt":            loc.get("altitude"),
            "country":        loc.get("country"),
            "indoor":         loc.get("indoor"),
            "exact_location": loc.get("exact_location"),
        },

        # Sensor hardware sub-object
        "device": {
            "sensor_id": sensor.get("id"),
            "pin":       sensor.get("pin"),
            "type": {
                "name":         stype.get("name"),          # e.g. "SDS011"
                "manufacturer": stype.get("manufacturer"),
            },
        },

        # Raw measurement readings dict (P1=PM10, P2=PM2.5, etc.)
        "measurements": vals,

        "tags": ["bronze", "telemetry"],
    }


# ── Kafka producer ────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

def make_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks":               "all",
        "retries":            5,
        "linger.ms":          20,
        "batch.num.messages": 1000,
    })

def choose_partition_key(event: Dict[str, Any]) -> str:
    """
    Partition key for TenantB: prefer location_id.
    Ensures all readings from the same location go to the same Kafka partition.
    """
    geo  = event.get("geo")    or {}
    dev  = event.get("device") or {}
    meta = event.get("meta")   or {}
    key  = geo.get("location_id") or dev.get("sensor_id") or meta.get("event_id")
    return str(key)

def dedupe_key(event: Dict[str, Any]) -> Tuple[Any, Any]:
    """Dedup key for TenantB: (location_id, timestamp_str)."""
    geo  = event.get("geo")  or {}
    meta = event.get("meta") or {}
    return (geo.get("location_id"), meta.get("timestamp_str"))


# ── main loop ─────────────────────────────────────────────────────────────────

def main():
    p    = make_kafka_producer()
    seen = set()

    print(f"==> TenantB producer started. Kafka={KAFKA_BOOTSTRAP}, topic={TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            obj   = fetch_random_object()
            event = build_tenantB_event(obj)

            # Dedup: skip if (location_id, timestamp) already sent
            dk = dedupe_key(event)
            if dk in seen:
                time.sleep(0.2)
                continue
            seen.add(dk)
            if len(seen) > DEDUP_MAX_SIZE:
                seen.clear()

            key = choose_partition_key(event)
            p.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                callback=delivery_report,
            )
            p.poll(0)

            geo  = event.get("geo")    or {}
            dev  = event.get("device") or {}
            meta = event.get("meta")   or {}
            print(f"TenantB → topic={TOPIC} key={key} "
                  f"ts={meta.get('timestamp_str')} "
                  f"location_id={geo.get('location_id')} "
                  f"sensor_id={dev.get('sensor_id')}")
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n==> Stopping TenantB producer...")
    finally:
        p.flush(10)
        print("✅ TenantB producer stopped.")


if __name__ == "__main__":
    main()
