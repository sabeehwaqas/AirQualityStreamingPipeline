#!/usr/bin/env python3
"""
streamingestmonitor.py  —  mysimbdp Streaming Ingest Monitor
=============================================================

WHAT THIS FILE DOES
-------------------
The monitor is the observability layer for Part 1 streaming ingestion.
It runs as a FastAPI service (port 8002) and:

  1. Receives KPI reports from worker containers every N seconds
  2. Persists each report to Cassandra (platform_logs.streaming_metrics)
  3. Checks if avg_ingest_ms exceeds MAX_AVG_INGEST_MS threshold
  4. If the threshold is breached, fires an alert to streamingestmanager

DATA FLOW
---------
  streamingestworker  →  POST /report  →  persist to Cassandra
                                       →  threshold check
                                           → if breach: POST alert to manager

CASSANDRA TABLE
---------------
  platform_logs.streaming_metrics:
    tenant_id, worker_id, ts, window_sec, avg_ingest_ms, records, bytes, errors

ALERT THRESHOLD (configurable via docker-compose.yaml)
-------------------------------------------------------
  MAX_AVG_INGEST_MS (default: 50ms)
    If avg_ingest_ms in a window > this, send alert to manager.

  MIN_RECORDS_PER_WINDOW (default: 1)
    Skip alerting on empty windows (no messages = no latency to measure).

ASSIGNMENT NOTE
---------------
  This satisfies P1.4 (report format + monitoring mechanism) and
  P1.5 (monitor receives report, checks threshold, alerts manager).
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import requests
from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ── configuration ─────────────────────────────────────────────────────────────
APP_PORT       = int(os.getenv("MONITOR_PORT",    "8002"))
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST",      "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT",  "9042"))
MANAGER_URL    = os.getenv("MANAGER_URL",         "http://streamingestmanager:8001")

# Threshold: if any window's avg_ingest_ms exceeds this, fire an alert
MAX_AVG_INGEST_MS     = float(os.getenv("MAX_AVG_INGEST_MS",      "50.0"))
# Don't alert on empty windows — only alert when actual records were processed
MIN_RECORDS_PER_WINDOW = int(os.getenv("MIN_RECORDS_PER_WINDOW",  "1"))

# Cassandra retry config — exponential backoff between connection attempts
RETRY_INITIAL_SEC = float(os.getenv("CASSANDRA_RETRY_INITIAL_SEC", "0.5"))
RETRY_MAX_SEC     = float(os.getenv("CASSANDRA_RETRY_MAX_SEC",     "10.0"))

# ── global Cassandra state ────────────────────────────────────────────────────
cluster:         Optional[Cluster] = None
session                            = None
prepared_insert                    = None
cassandra_ready: bool              = False


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── request model ─────────────────────────────────────────────────────────────

class WorkerReport(BaseModel):
    """
    KPI report sent by streamingestworker every window_sec seconds.

    This is the standard report format the worker must follow (blackbox contract).
    The monitor validates these fields and rejects malformed reports.
    """
    tenant_id:     str   = Field(..., min_length=1)   # which tenant sent this
    worker_id:     str   = Field(..., min_length=1)   # container/consumer group ID
    ts:            datetime                           # timestamp of report
    window_sec:    int   = Field(..., ge=1)           # reporting window duration
    avg_ingest_ms: float = Field(..., ge=0)           # avg ms per record (KEY METRIC)
    records:       int   = Field(..., ge=0)           # records successfully written
    bytes:         int   = Field(..., ge=0)           # total bytes ingested
    errors:        int   = Field(..., ge=0)           # parse + write failures


# ── Cassandra connection with async retry ─────────────────────────────────────

async def _connect_cassandra_with_retry():
    """
    Connects to Cassandra and prepares the INSERT statement.
    Runs as an async background task — retries with exponential backoff
    so the monitor starts up and serves /health even before Cassandra is ready.
    """
    global cluster, session, prepared_insert, cassandra_ready
    delay = RETRY_INITIAL_SEC

    while True:
        try:
            if cluster is None:
                cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()

            # Prepare parameterized INSERT for streaming metrics
            # Prepared statements are compiled once and reused — more efficient
            # and safer than string-formatted CQL
            prepared_insert = session.prepare("""
                INSERT INTO platform_logs.streaming_metrics
                (tenant_id, worker_id, ts, window_sec, avg_ingest_ms, records, bytes, errors)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """)
            cassandra_ready = True
            print(f"✅ Monitor connected to Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return

        except NoHostAvailable:
            cassandra_ready = False
            print(f"⏳ Cassandra not reachable. Retrying in {delay:.1f}s...")
        except Exception as e:
            cassandra_ready = False
            print(f"⏳ Cassandra connect error ({type(e).__name__}). Retrying in {delay:.1f}s...")

        await asyncio.sleep(delay)
        delay = min(RETRY_MAX_SEC, delay * 2)   # exponential backoff, capped at RETRY_MAX_SEC


# ── alert sender ──────────────────────────────────────────────────────────────

def _send_alert_best_effort(alert_payload: dict) -> None:
    """
    Send a threshold alert to streamingestmanager.

    BEST-EFFORT: we do NOT block the /report response on manager connectivity.
    If the manager is temporarily down, we log a warning and continue.
    Monitoring must never impact ingestion performance.
    """
    try:
        r = requests.post(
            f"{MANAGER_URL.rstrip('/')}/monitor/alerts",
            json=alert_payload,
            timeout=2.0,
        )
        if r.status_code >= 300:
            print(f"⚠️ manager rejected alert: status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        print(f"⚠️ failed to send alert to manager: {type(e).__name__}: {e}")


# ── FastAPI lifecycle ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    On startup: launch Cassandra connection task in background.
    On shutdown: cancel task + close cluster connection.
    """
    task = asyncio.create_task(_connect_cassandra_with_retry())
    try:
        yield
    finally:
        task.cancel()
        global cluster
        try:
            if cluster:
                cluster.shutdown()
        except Exception:
            pass


app = FastAPI(title="mysimbdp-streamingestmonitor", lifespan=lifespan)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """
    Liveness + readiness check.
    cassandra_ready=false is normal during startup — Cassandra is still connecting.
    """
    return {
        "ok":                         True,
        "cassandra_ready":            cassandra_ready,
        "cassandra":                  f"{CASSANDRA_HOST}:{CASSANDRA_PORT}",
        "manager_url":                MANAGER_URL,
        "threshold_max_avg_ingest_ms": MAX_AVG_INGEST_MS,
    }


@app.post("/report")
def report(r: WorkerReport):
    """
    Receive a KPI window report from a worker container.

    Steps:
      1. Validate the report (Pydantic does this automatically)
      2. Persist to Cassandra (platform_logs.streaming_metrics)
      3. Check avg_ingest_ms against threshold
      4. If threshold breached AND window had actual records → send alert to manager

    Returns HTTP 503 if Cassandra isn't ready yet (monitor is still connecting).
    """
    if not cassandra_ready or prepared_insert is None:
        raise HTTPException(status_code=503,
                            detail="Cassandra not ready — monitor is retrying connection")

    # ── Persist KPI to Cassandra ───────────────────────────────────────────────
    try:
        session.execute(
            prepared_insert,
            (
                r.tenant_id,
                r.worker_id,
                r.ts,
                r.window_sec,
                float(r.avg_ingest_ms),
                int(r.records),
                int(r.bytes),
                int(r.errors),
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"Failed to persist report: {type(e).__name__}: {e}")

    # ── Threshold check → alert manager if breached ───────────────────────────
    #
    # Only alert when:
    #   - We had at least MIN_RECORDS_PER_WINDOW records (not an idle window)
    #   - avg_ingest_ms exceeded the configured threshold
    #
    # The alert includes suggested_action="restart" — the manager may or may
    # not act on this depending on its own logic.
    if r.records >= MIN_RECORDS_PER_WINDOW and r.avg_ingest_ms > MAX_AVG_INGEST_MS:
        alert = {
            "tenant_id":        r.tenant_id,
            "worker_id":        r.worker_id,
            "ts":               iso_utc_now(),
            "issue":            "avg_ingest_ms_high",          # what went wrong
            "avg_ingest_ms":    float(r.avg_ingest_ms),        # measured value
            "records":          int(r.records),
            "window_sec":       int(r.window_sec),
            "threshold":        float(MAX_AVG_INGEST_MS),      # what the limit is
            "suggested_action": "restart",                     # hint to manager
        }
        _send_alert_best_effort(alert)

    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
