#!/usr/bin/env python3
"""
streamingestworker.py  —  mysimbdp Streaming Ingest Worker
===========================================================

WHAT THIS FILE DOES
-------------------
This is the core ingestion engine. One container of this runs per tenant.
It:
  1. Connects to the Kafka broker and subscribes to the tenant's topic
     (e.g. tenantA.bronze.raw)
  2. Consumes messages one by one, parses the relevant ID/timestamp fields
  3. Inserts each raw message as a row into the tenant's Bronze Cassandra table
  4. Every N seconds (window), calculates average ingest latency and sends
     a KPI report to streamingestmonitor via HTTP POST

WHO STARTS THIS?
----------------
  streamingestmanager.py starts/stops this as a Docker container via the Docker SDK.
  You can also start it manually:
    python streamingestworker.py --tenant tenantA --cassandra 127.0.0.1

BLACKBOX CONTRACT (what streamingestmanager expects)
----------------------------------------------------
  - Accepts: --tenant, --kafka, --cassandra, --group, --monitor-url, --report-window-sec
  - Runs forever until stopped
  - Reports JSON KPIs to /report on streamingestmonitor

DATA FLOW
---------
  Kafka topic (tenantA.bronze.raw)
      → parse message (extract sensor_id / location_id, event_ts, event_id)
      → INSERT INTO tenantA_bronze.records (sensor_id, ingest_ts, event_ts, event_id, topic, payload)
      → every 10s: POST KPI report to streamingestmonitor
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from confluent_kafka import Consumer
from cassandra.cluster import Cluster


# ── timestamp helpers ──────────────────────────────────────────────────────────

def now_utc() -> datetime:
    """Returns the current time as a timezone-aware UTC datetime object."""
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    """Converts a datetime to ISO 8601 string e.g. '2026-03-01T12:00:10.123456+00:00'."""
    return dt.isoformat()


# ── tenant-specific message parsers ───────────────────────────────────────────
#
# Each parser extracts the three fields the worker needs to write to Cassandra:
#   pk        = the primary key (sensor_id for tenantA, location_id for tenantB)
#   event_ts  = the timestamp from the source API (as a string)
#   event_id  = the numeric event ID from the source
#
# WHY SEPARATE PARSERS?
# TenantA uses a flat JSON structure (everything at the top level).
# TenantB uses a nested structure (geo/meta/device sub-objects).
# The worker doesn't need to understand the full schema — it only extracts
# the fields needed for the Cassandra PK and metadata. The full raw payload
# is stored as-is for the silver pipeline to process later.

def parse_tenantA(payload_str: str) -> Dict[str, Any]:
    """
    Parse a TenantA Kafka message.

    TenantA uses a FLAT schema — all fields at the top level:
      { "sensor_id": 123, "timestamp": "2026-...", "event_id": 456, "pm10_P1": 12.3, ... }

    Returns dict with keys: pk, event_ts, event_id
    """
    out = {"pk": "unknown", "event_ts": None, "event_id": 0}
    obj = json.loads(payload_str)
    out["pk"]       = str(obj.get("sensor_id") or "unknown")   # Cassandra PK column
    out["event_ts"] = obj.get("timestamp")                      # Source timestamp string
    out["event_id"] = int(obj.get("event_id") or 0)             # Numeric event ID
    return out


def parse_tenantB(payload_str: str) -> Dict[str, Any]:
    """
    Parse a TenantB Kafka message.

    TenantB uses a NESTED schema with geo and meta sub-objects:
      {
        "geo": { "location_id": 789, ... },
        "meta": { "timestamp_str": "2026-...", "event_id": 456 },
        "device": { "sensor_id": 123, ... },
        "measurements": { "P1": "12.3", ... }
      }

    Returns dict with keys: pk, event_ts, event_id
    """
    out  = {"pk": "unknown", "event_ts": None, "event_id": 0}
    obj  = json.loads(payload_str)
    geo  = obj.get("geo")  or {}  # Geo sub-object — contains location_id
    meta = obj.get("meta") or {}  # Meta sub-object — contains timestamp + event_id
    out["pk"]       = str(geo.get("location_id") or "unknown")
    out["event_ts"] = meta.get("timestamp_str")
    out["event_id"] = int(meta.get("event_id") or 0)
    return out


# ── monitor reporting ──────────────────────────────────────────────────────────

def send_report(monitor_url: str, report: Dict[str, Any]) -> None:
    """
    Send a KPI window report to streamingestmonitor via HTTP POST.

    This is BEST-EFFORT — if the monitor is down, we log a warning and continue.
    Monitoring failures must NEVER crash the ingestion loop.

    Report payload contains:
      tenant_id, worker_id, ts, window_sec,
      avg_ingest_ms, records, bytes, errors
    """
    try:
        r = requests.post(
            f"{monitor_url.rstrip('/')}/report",
            json=report,
            timeout=2.0,   # short timeout so we don't block the ingest loop
        )
        if r.status_code >= 300:
            print(f"⚠️ monitor rejected report: status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        print(f"⚠️ monitor report failed: {type(e).__name__}: {e}")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    # ── CLI arguments (also settable via environment variables) ───────────────
    parser = argparse.ArgumentParser(description="mysimbdp streaming ingest worker")
    parser.add_argument("--tenant",    required=True, choices=["tenantA", "tenantB"],
                        help="Which tenant to ingest for (determines topic + keyspace)")
    parser.add_argument("--kafka",     default=os.getenv("KAFKA_BOOTSTRAP", "broker:29092"),
                        help="Kafka bootstrap server. Must use internal port 29092 inside Docker")
    parser.add_argument("--cassandra", default="cassandra",
                        help="Cassandra hostname (use 'cassandra' inside Docker)")
    parser.add_argument("--group",     default=None,
                        help="Kafka consumer group ID (defaults to <tenant>-streamingestworker)")
    parser.add_argument("--monitor-url",        default=os.getenv("MONITOR_URL", ""),
                        help="URL of streamingestmonitor (e.g. http://streamingestmonitor:8002)")
    parser.add_argument("--report-window-sec",  type=int,
                        default=int(os.getenv("REPORT_WINDOW_SEC", "10")),
                        help="How often to send KPI reports to the monitor (seconds)")
    args = parser.parse_args()

    tenant = args.tenant

    # ── Tenant routing: set topic, keyspace, primary key column, parser ───────
    #
    # SHARED: Same Kafka broker, same Cassandra cluster, same Docker image
    # DEDICATED PER TENANT: separate topic, separate keyspace, separate consumer group
    #
    # This is the multi-tenancy design: isolation at the data layer,
    # shared infrastructure to reduce operational overhead.
    if tenant == "tenantA":
        topic    = "tenantA.bronze.raw"   # Dedicated Kafka topic for tenantA
        keyspace = "tenantA_bronze"       # Dedicated Cassandra keyspace
        table    = "records"
        pk_col   = "sensor_id"            # PK column name in Cassandra
        parse_fn = parse_tenantA          # Flat schema parser
    else:
        topic    = "tenantB.bronze.raw"   # Dedicated Kafka topic for tenantB
        keyspace = "tenantB_bronze"       # Dedicated Cassandra keyspace
        table    = "records"
        pk_col   = "location_id"          # PK column name in Cassandra
        parse_fn = parse_tenantB          # Nested schema parser

    # Consumer group ID is dedicated per tenant — ensures each worker has
    # its own independent offset tracking in Kafka
    group_id    = args.group or f"{tenant}-streamingestworker"
    monitor_url = (args.monitor_url or "").strip()
    window_sec  = max(1, int(args.report_window_sec))

    # ── Cassandra connection with retry ───────────────────────────────────────
    #
    # WHY RETRY?
    # This worker container may be started before Cassandra has finished
    # initializing (even after the Docker healthcheck passes, the schema
    # tables may not exist yet). We retry up to 12 times with 5s between
    # attempts to survive this race condition.
    cluster = None
    session = None
    for _attempt in range(1, 13):
        try:
            cluster = Cluster([args.cassandra])
            session = cluster.connect()

            # Verify schema is ready by running a test query against the
            # specific bronze table. This confirms the keyspace + table exist,
            # not just that Cassandra is accepting connections.
            insert_cql_test = (
                f"SELECT sensor_id FROM tenantA_bronze.records LIMIT 1"
                if tenant == "tenantA" else
                f"SELECT location_id FROM tenantB_bronze.records LIMIT 1"
            )
            session.execute(insert_cql_test)
            print(f"✅ Cassandra connected (attempt {_attempt})")
            break
        except Exception as e:
            print(f"⏳ Cassandra not ready (attempt {_attempt}/12): "
                  f"{type(e).__name__}: {e}. Retrying in 5s...")
            if _attempt == 12:
                print("❌ Could not connect to Cassandra after 12 attempts. Exiting.")
                raise
            time.sleep(5)

    # ── Prepare parameterized Cassandra INSERT ─────────────────────────────────
    #
    # We prepare once at startup for efficiency — Cassandra compiles the query
    # once and reuses the compiled form on every execute() call.
    # Columns: PK (sensor_id or location_id), ingest_ts (platform timestamp),
    #          event_ts (source timestamp), event_id, topic name, raw payload
    insert_cql = f"""
    INSERT INTO {keyspace}.{table}
    ({pk_col}, ingest_ts, event_ts, event_id, topic, payload)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_cql)

    # ── Kafka consumer setup ───────────────────────────────────────────────────
    #
    # auto.offset.reset = "latest": only consume messages produced AFTER
    # this consumer starts. We don't want to reprocess historical messages
    # on restart — the silver pipeline handles deduplication via watermarks.
    consumer = Consumer({
        "bootstrap.servers": args.kafka,
        "group.id":          group_id,
        "auto.offset.reset": "latest",   # start from newest messages on first run
        "enable.auto.commit": True,       # commit offsets automatically after poll
    })
    consumer.subscribe([topic])

    print(f"==> streamingestworker started: tenant={tenant} topic={topic} "
          f"kafka={args.kafka} cassandra={args.cassandra}")
    if monitor_url:
        print(f"==> KPI reporting enabled → {monitor_url} every {window_sec}s")
    else:
        print("==> KPI reporting disabled (no MONITOR_URL set)")
    print("Press Ctrl+C to stop.\n")

    # ── Window metrics — reset every window_sec seconds ───────────────────────
    #
    # These accumulate stats for the current reporting window.
    # When the window closes, averages are computed and sent to the monitor.
    win_start        = time.time()
    win_records      = 0       # successful inserts this window
    win_bytes        = 0       # total bytes consumed this window
    win_errors       = 0       # parse + insert errors this window
    win_sum_ingest_ms = 0.0    # sum of per-record ingest latency (ms)

    # ── Main consume loop ──────────────────────────────────────────────────────
    try:
        while True:
            # poll() blocks for up to 1 second waiting for a message.
            # Returns None if no message arrived in that window.
            msg = consumer.poll(1.0)
            now = time.time()

            # No message arrived — check if we need to flush the report window
            if msg is None:
                if monitor_url and (now - win_start) >= window_sec:
                    _flush_report(monitor_url, tenant, group_id, window_sec,
                                  win_records, win_bytes, win_errors, win_sum_ingest_ms)
                    win_start, win_records, win_bytes, win_errors, win_sum_ingest_ms = \
                        now, 0, 0, 0, 0.0
                continue

            # Kafka returned an error (e.g. broker disconnected, partition rebalance)
            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                win_errors += 1
                continue

            t0 = time.time()   # start timing this record's ingest latency

            # ── Decode message ─────────────────────────────────────────────────
            payload_bytes = msg.value()
            payload_str   = payload_bytes.decode("utf-8", errors="replace")

            # ── Parse message to extract PK + metadata ─────────────────────────
            # Even if parsing fails, we STILL ingest the raw payload.
            # Bronze is a raw store — we want 100% capture, parsing issues
            # are handled at the silver layer.
            pk       = "unknown"
            event_ts: Optional[str] = None
            event_id = 0
            try:
                parsed   = parse_fn(payload_str)
                pk       = parsed["pk"]
                event_ts = parsed["event_ts"]
                event_id = parsed["event_id"]
            except Exception:
                win_errors += 1   # count as parse error, but continue below

            # Capture platform ingest timestamp (when WE received + stored it)
            ingest_ts = now_utc()

            # ── Insert into Cassandra bronze table ─────────────────────────────
            # If insert fails, log and continue — never crash the consume loop.
            try:
                session.execute(
                    prepared,
                    (pk, ingest_ts, event_ts, event_id, topic, payload_str)
                )
            except Exception as e:
                win_errors += 1
                print(f"❌ Cassandra insert failed: {type(e).__name__}: {e}")
                continue   # skip metrics update for this failed record

            # ── Update window metrics ──────────────────────────────────────────
            elapsed_ms = (time.time() - t0) * 1000.0
            win_records      += 1
            win_bytes        += len(payload_bytes)
            win_sum_ingest_ms += elapsed_ms

            print(f"{tenant} → {pk_col}={pk} event_ts={event_ts} "
                  f"bytes={len(payload_bytes)} ingest_ms={elapsed_ms:.2f}")

            # ── Flush report window if elapsed ─────────────────────────────────
            now2 = time.time()
            if monitor_url and (now2 - win_start) >= window_sec:
                _flush_report(monitor_url, tenant, group_id, window_sec,
                              win_records, win_bytes, win_errors, win_sum_ingest_ms)
                win_start, win_records, win_bytes, win_errors, win_sum_ingest_ms = \
                    now2, 0, 0, 0, 0.0

    except KeyboardInterrupt:
        print("\n==> Stopping worker...")

    finally:
        # Always clean up Kafka + Cassandra connections on exit
        try:
            consumer.close()
        finally:
            cluster.shutdown()
        print("✅ Worker stopped cleanly.")


def _flush_report(monitor_url, tenant, group_id, window_sec,
                  win_records, win_bytes, win_errors, win_sum_ingest_ms):
    """Build and send one KPI window report to the monitor."""
    avg_ms = (win_sum_ingest_ms / win_records) if win_records > 0 else 0.0
    report = {
        "tenant_id":     tenant,
        "worker_id":     group_id,
        "ts":            iso_utc(now_utc()),
        "window_sec":    window_sec,
        "avg_ingest_ms": avg_ms,        # average time per record in ms — monitor checks this
        "records":       win_records,   # total successful inserts in this window
        "bytes":         win_bytes,     # total payload bytes ingested
        "errors":        win_errors,    # parse + insert failures
    }
    send_report(monitor_url, report)


if __name__ == "__main__":
    main()
