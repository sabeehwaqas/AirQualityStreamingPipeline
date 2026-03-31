#!/usr/bin/env python3
"""
streamingestmanager.py  —  mysimbdp Streaming Ingest Manager
=============================================================

WHAT THIS FILE DOES
-------------------
The manager is the control plane for Part 1 streaming ingestion.
It exposes a REST API (FastAPI, port 8001) for starting and stopping
worker containers on demand. It also receives threshold alerts from
the monitor and can restart workers in response.

KEY RESPONSIBILITIES
--------------------
  1. Start/stop streamingestworker Docker containers per tenant
  2. Track worker state in a JSON file (/app/state/workers.json)
  3. Receive monitor alerts at POST /monitor/alerts
  4. Optionally restart a worker container when the monitor says so

HOW WORKERS ARE STARTED
-----------------------
  Worker containers are launched using the Docker SDK (docker Python library).
  The manager mounts the Docker socket (/var/run/docker.sock) to control
  containers from inside a container — this is a standard pattern called
  "Docker-in-Docker via socket".

  Each worker gets:
    - The same Docker image (mysimbdp-platform:latest)
    - Its own container name (e.g. streamingestworker-tenantA)
    - Environment variables: MONITOR_URL, REPORT_WINDOW_SEC
    - The correct --tenant, --kafka, --cassandra flags

  UNDER-PROVISIONING TEST (P1.3):
    Uncomment mem_limit and cpu_quota below to throttle the worker container.
    This simulates resource contention and should trigger monitor alerts.

MULTI-TENANCY DESIGN
--------------------
  Shared:   Kafka broker, Cassandra cluster, Docker image, manager, monitor
  Dedicated per tenant: worker container, Kafka topic, Cassandra keyspace,
                        Kafka consumer group ID

API ENDPOINTS
-------------
  GET  /health                              — liveness check
  GET  /tenants/{tenant_id}/workers         — list worker containers for tenant
  POST /tenants/{tenant_id}/workers/start   — start a worker container
  POST /tenants/{tenant_id}/workers/stop    — stop a worker container
  GET  /monitor/alerts                      — view recent alerts from monitor
  POST /monitor/alerts                      — receive alert from monitor (internal)
"""

import json
import os
import time
from typing import Dict, Any

import docker
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ── configuration (from environment variables set in docker-compose.yaml) ─────
APP_PORT          = 8001
MONITOR_URL       = os.getenv("MONITOR_URL",       "http://streamingestmonitor:8002")
REPORT_WINDOW_SEC = os.getenv("REPORT_WINDOW_SEC", "10")  # passed to worker containers
STATE_PATH        = os.getenv("STATE_PATH",        "/app/state/workers.json")
IMAGE_NAME        = os.getenv("WORKER_IMAGE",      "mysimbdp-platform:latest")
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",   "broker:9092")  # internal broker address
CASSANDRA_HOST    = os.getenv("CASSANDRA_HOST",    "cassandra")

# ── tenant → Kafka topic mapping ─────────────────────────────────────────────
# Only known tenant IDs can start workers — others are rejected with HTTP 400
TENANT_TOPICS = {
    "tenantA": "tenantA.bronze.raw",
    "tenantB": "tenantB.bronze.raw",
}

app = FastAPI(title="mysimbdp-streamingestmanager")


# ── request/response models ───────────────────────────────────────────────────

class StartReq(BaseModel):
    instances: int = 1   # reserved for future multi-instance support

class StopReq(BaseModel):
    instances: int = 1

class MonitorAlert(BaseModel):
    """
    Alert payload sent by streamingestmonitor when KPIs breach a threshold.
    Fields mirror the alert JSON in streamingestmonitor.py.
    """
    tenant_id:        str
    worker_id:        str
    ts:               str
    issue:            str              # e.g. "avg_ingest_ms_high"
    avg_ingest_ms:    float | None = None
    records:          int   | None = None
    window_sec:       int   | None = None
    threshold:        float | None = None
    suggested_action: str   | None = None  # e.g. "restart"


# ── state management ──────────────────────────────────────────────────────────
#
# Worker state is persisted to a JSON file on disk so it survives container
# restarts. Alerts are also stored here so they can be queried via GET /monitor/alerts.

def load_state() -> Dict[str, Any]:
    """Load worker state + alerts from disk. Returns empty state if file missing."""
    if not os.path.exists(STATE_PATH):
        return {"workers": {}, "alerts": []}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        s = json.load(f)
    # Ensure both keys always exist, even in old state files
    if "alerts"  not in s: s["alerts"]  = []
    if "workers" not in s: s["workers"] = {}
    return s

def save_state(state: Dict[str, Any]) -> None:
    """Persist worker state + alerts to disk."""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ── Docker network detection ──────────────────────────────────────────────────

def get_compose_network_name() -> str:
    """
    Auto-detect the docker-compose network this manager container is attached to.

    Worker containers MUST be on the same network so they can reach:
      - broker:29092 (Kafka)
      - cassandra:9042 (Cassandra)
      - streamingestmonitor:8002 (Monitor)

    This works by inspecting the manager container's own network settings.
    The hostname inside Docker equals the container name (os.uname().nodename).
    """
    client = docker.from_env()
    me     = client.containers.get(os.uname().nodename)
    nets   = list(me.attrs["NetworkSettings"]["Networks"].keys())
    if not nets:
        raise RuntimeError("Manager is not attached to a docker network.")
    return nets[0]  # use the first (and typically only) network


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Simple liveness check — returns ok + config."""
    return {"ok": True, "monitor_url": MONITOR_URL}


@app.get("/tenants/{tenant_id}/workers")
def list_workers(tenant_id: str):
    """List the last-known state of worker containers for a tenant."""
    state = load_state()
    return {"tenant": tenant_id, "workers": state["workers"].get(tenant_id, {})}


@app.get("/monitor/alerts")
def list_alerts(limit: int = 50):
    """Return the most recent monitor alerts (newest last)."""
    state = load_state()
    return {"alerts": state["alerts"][-limit:]}


@app.post("/tenants/{tenant_id}/workers/start")
def start_workers(tenant_id: str, req: StartReq):
    """
    Start a worker container for the given tenant.

    IDEMPOTENT: If the container already exists and is running, does nothing.
    If it exists but is stopped, starts it. If it doesn't exist, creates it.

    The worker is configured via:
      - Command-line args: --tenant, --kafka, --cassandra
      - Environment variables: MONITOR_URL, REPORT_WINDOW_SEC

    P1.3 UNDER-PROVISIONING TEST:
      Uncomment mem_limit and cpu_quota to throttle the container.
      This will slow down Cassandra inserts, raise avg_ingest_ms, and
      cause the monitor to fire threshold alerts to this manager.
    """
    if tenant_id not in TENANT_TOPICS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown tenant_id. Use one of {list(TENANT_TOPICS.keys())}"
        )

    client         = docker.from_env()
    network        = get_compose_network_name()
    worker_id      = f"{tenant_id}-worker"
    container_name = f"streamingestworker-{tenant_id}"

    # Command the worker container will run
    cmd = [
        "python", "streamingestworker.py",
        "--tenant",    tenant_id,
        "--kafka",     KAFKA_BOOTSTRAP,
        "--cassandra", CASSANDRA_HOST,
    ]

    # If container already exists, start it (idempotent)
    try:
        c = client.containers.get(container_name)
        c.reload()
        if c.status != "running":
            c.start()
        cid = c.id

    # Container doesn't exist yet — create + start it
    except docker.errors.NotFound:
        c = client.containers.run(
            IMAGE_NAME,
            name=container_name,
            command=cmd,
            detach=True,             # run in background
            network=network,         # same network as broker/cassandra/monitor
            restart_policy={"Name": "unless-stopped"},  # auto-restart on crash
            environment={
                "MONITOR_URL":        MONITOR_URL,        # where to send KPI reports
                "REPORT_WINDOW_SEC":  REPORT_WINDOW_SEC,  # how often to report
            },
            # ── P1.3: Uncomment to test under-provisioned workers ─────────────
            # Throttle memory and CPU to simulate an overloaded worker.
            # This raises avg_ingest_ms, triggering monitor threshold alerts.
            mem_limit="128m",      # max 128 MB RAM
            cpu_quota=10000,       # 10% of 1 CPU (100000 = full CPU)
        )
        cid = c.id

    # Persist worker state to disk
    state = load_state()
    state["workers"].setdefault(tenant_id, {})
    state["workers"][tenant_id][worker_id] = {
        "container_name": container_name,
        "container_id":   cid,
        "status":         "running",
        "started_at":     int(time.time()),
        "kafka":          KAFKA_BOOTSTRAP,
        "cassandra":      CASSANDRA_HOST,
        "monitor_url":    MONITOR_URL,
    }
    save_state(state)

    return {"tenant": tenant_id, "started": [{"worker_id": worker_id, "container": container_name}]}


@app.post("/tenants/{tenant_id}/workers/stop")
def stop_workers(tenant_id: str, req: StopReq):
    """
    Stop the worker container for the given tenant.
    Graceful stop (5 second timeout before SIGKILL).
    """
    client         = docker.from_env()
    container_name = f"streamingestworker-{tenant_id}"

    try:
        c = client.containers.get(container_name)
        c.reload()
        if c.status == "running":
            c.stop(timeout=5)   # give worker 5s to finish current message
        stopped = True
    except docker.errors.NotFound:
        stopped = False   # container doesn't exist — that's fine

    # Update state file
    state = load_state()
    wid   = f"{tenant_id}-worker"
    if tenant_id in state.get("workers", {}) and wid in state["workers"][tenant_id]:
        state["workers"][tenant_id][wid]["status"]     = "stopped"
        state["workers"][tenant_id][wid]["stopped_at"] = int(time.time())
        save_state(state)

    return {"tenant": tenant_id, "stopped": stopped, "container": container_name}


@app.post("/monitor/alerts")
def receive_alert(a: MonitorAlert):
    """
    Receive a threshold alert from streamingestmonitor.

    The monitor calls this endpoint when avg_ingest_ms > MAX_AVG_INGEST_MS.
    We:
      1. Store the alert in state for later inspection (GET /monitor/alerts)
      2. Optionally restart the worker container if suggested_action == "restart"

    ASSIGNMENT NOTE: receiving + recording the alert satisfies P1.5.
    The optional restart demonstrates auto-remediation on threshold breach.
    """
    state = load_state()
    state["alerts"].append(a.model_dump())
    save_state(state)

    # Optional auto-remediation: restart the worker if monitor suggests it
    action_taken = None
    if (a.suggested_action or "").lower() == "restart":
        client         = docker.from_env()
        container_name = f"streamingestworker-{a.tenant_id}"
        try:
            c = client.containers.get(container_name)
            c.reload()
            c.restart(timeout=5)
            action_taken = f"restarted {container_name}"
        except docker.errors.NotFound:
            action_taken = f"worker container {container_name} not found"

    return {"ok": True, "action_taken": action_taken}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
