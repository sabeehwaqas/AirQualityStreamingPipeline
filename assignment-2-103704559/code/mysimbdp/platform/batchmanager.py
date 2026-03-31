#!/usr/bin/env python3
"""
batchmanager.py  —  mysimbdp Batch Manager (Part 2)
====================================================

WHAT THIS FILE DOES
-------------------
The batchmanager is the orchestrator for Part 2 silver batch pipelines.
It runs as a FastAPI service (port 8003) and:

  1. Reads tenant service agreements from /app/tenant_configs/*.yaml
  2. Schedules silverpipeline runs every N minutes per tenant (APScheduler)
  3. Before each run, checks constraints defined in the service agreement
  4. If all constraints pass, launches the silverpipeline as a subprocess
  5. Reads the structured JSON "complete" line from pipeline stdout
  6. Persists full run metrics to Cassandra (platform_logs.silver_pipeline_logs)
  7. Exposes /stats endpoint for platform-wide performance analytics

BLACKBOX PIPELINE MODEL
-----------------------
  The batchmanager treats each silver pipeline as a BLACK BOX.
  It only knows:
    - How to invoke it (python silverpipeline_<tenant>.py --run-id ... --max-records ...)
    - That exit 0 = success, exit 1 = failure
    - That stdout contains JSON lines, and the "stage=complete" line has all metrics
  
  This design isolates the platform from tenant-specific pipeline logic.
  Tenants can change their pipeline internals without modifying batchmanager.

CONSTRAINTS (enforced BEFORE launching the pipeline)
-----------------------------------------------------
  reject_if_silver_table_missing  — check the silver Cassandra table exists
  reject_if_pending_overflow      — check pending dir hasn't exceeded max files
  require_non_empty_bronze        — check bronze table has records
  max_file_size_mb                — check pending files aren't too large
  require_geo_fields              — (tenantB) sample bronze rows for lat/lon

CASSANDRA TABLE
---------------
  platform_logs.silver_pipeline_logs:
    tenant_id, run_id, started_at, finished_at, status,
    records_loaded, errors, elapsed_sec,
    extract_sec, transform_sec, data_size_bytes, cache_mode,
    pipeline_script, detail

API ENDPOINTS
-------------
  GET  /health                    — readiness + cassandra status
  GET  /tenants                   — list registered tenants + schedules
  POST /tenants/{id}/run          — manually trigger a pipeline run
  GET  /tenants/{id}/logs         — recent run history for a tenant
  GET  /stats                     — platform-wide aggregated statistics
"""

import glob
import json
import os
import subprocess
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import FastAPI, HTTPException

# ── configuration (from docker-compose.yaml environment section) ──────────────
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST",     "cassandra")
TENANT_CONFIGS = os.getenv("TENANT_CONFIGS_DIR", "/app/tenant_configs")
APP_PORT       = int(os.getenv("BATCH_MANAGER_PORT", "8003"))
CACHE_BASE     = os.getenv("CACHE_BASE_DIR",     "/app/cache")

# ── global Cassandra session (shared by all endpoints + scheduler jobs) ───────
_session        = None
_prepared_log   = None    # prepared INSERT statement for silver_pipeline_logs
_cassandra_ok   = False   # flag: is Cassandra connected and schema ready?


# ── Cassandra connection ──────────────────────────────────────────────────────

def _connect_cassandra():
    """
    Connect to Cassandra and prepare the log INSERT statement.
    Retries up to 30 times with 5s between attempts.

    WHY RETRY UP TO 30 TIMES?
    The batchmanager starts immediately after docker compose up.
    Cassandra takes 60-90s to fully initialize. Without retries, batchmanager
    would fail to start and never recover.

    If session.prepare() fails, it usually means the silver_pipeline_logs table
    is missing the P2.4/P2.5 columns (extract_sec, transform_sec, etc.).
    This happens when an old Cassandra volume persists across a docker compose down.
    Fix: run bootstrap.sh (which has ALTER TABLE statements) or use -v flag.
    """
    global _session, _prepared_log, _cassandra_ok
    for attempt in range(1, 31):
        try:
            cluster   = Cluster([CASSANDRA_HOST])
            _session  = cluster.connect()

            # This prepare() will FAIL if silver_pipeline_logs is missing columns.
            # The error message will tell you exactly which column is missing.
            _prepared_log = _session.prepare("""
                INSERT INTO platform_logs.silver_pipeline_logs
                (tenant_id, run_id, started_at, finished_at, status,
                 records_loaded, errors, elapsed_sec,
                 extract_sec, transform_sec, data_size_bytes, cache_mode,
                 pipeline_script, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            _cassandra_ok = True
            print("✅ batchmanager connected to Cassandra")
            return

        except NoHostAvailable:
            # Cassandra process is not yet accepting TCP connections
            print(f"⏳ Cassandra host not reachable (attempt {attempt}/30). Waiting 5s...")
            time.sleep(5)
        except Exception as e:
            # Cassandra is up but schema is missing/wrong columns
            # Print the REAL error so you can diagnose it immediately
            print(f"⏳ Cassandra prepare failed (attempt {attempt}/30): {type(e).__name__}: {e}")
            time.sleep(5)

    print("❌ Could not connect to Cassandra after 30 attempts.")
    print("   Most likely cause: silver_pipeline_logs table is missing columns.")
    print("   Fix: run bootstrap.sh or fix_schema.sh, then restart batchmanager.")


def persist_log(
    tenant_id:       str,
    run_id:          str,
    started_at:      str,
    finished_at:     str,
    status:          str,        # "success" | "failed" | "constraint_violation" | "timeout"
    records_loaded:  int,
    errors:          int,
    elapsed_sec:     float,      # total wall time for the run
    extract_sec:     float,      # time for Stage 1 (bronze read + cache write)
    transform_sec:   float,      # time for Stage 2 (transform + silver insert)
    data_size_bytes: int,        # size of the .jsonl cache file written
    cache_mode:      str,        # "local" or "local+gcs"
    pipeline_script: str,        # e.g. "silverpipeline_tenantA.py"
    detail:          str,        # full pipeline stdout (last 4000 chars)
):
    """Write one run record to platform_logs.silver_pipeline_logs."""
    if not _cassandra_ok or _prepared_log is None:
        print(f"⚠️ Cassandra not ready — skipping log for run {run_id}")
        return
    try:
        _session.execute(_prepared_log, (
            tenant_id, run_id, started_at, finished_at, status,
            records_loaded, errors, elapsed_sec,
            extract_sec, transform_sec, data_size_bytes, cache_mode,
            pipeline_script, detail[:4000],   # cap at 4000 chars for Cassandra
        ))
    except Exception as e:
        print(f"⚠️ Failed to persist log for run {run_id}: {e}")


# ── tenant config loader ──────────────────────────────────────────────────────

def load_tenant_configs() -> Dict[str, dict]:
    """
    Load all *.yaml files from TENANT_CONFIGS_DIR.
    Each file is a service agreement for one tenant.
    Returns dict: { tenant_id -> config_dict }

    The YAML files define:
      - pipeline_script: which python file to invoke
      - scheduling.interval_minutes: how often to run
      - data_limits: max_records_per_run, max_file_size_mb, max_cache_files_pending
      - silver_table: Cassandra keyspace + table name
      - caching: pending_dir + processed_dir paths
      - constraints: which checks to enforce before each run
    """
    configs = {}
    for path in glob.glob(os.path.join(TENANT_CONFIGS, "*.yaml")):
        with open(path) as f:
            cfg = yaml.safe_load(f)
        tid = cfg.get("tenant_id")
        if tid:
            configs[tid] = cfg
            print(f"✅ Loaded config for tenant: {tid}")
    return configs


# ── constraint checker ────────────────────────────────────────────────────────

def check_constraints(tenant_id: str, cfg: dict) -> Optional[str]:
    """
    Evaluate all constraints defined in the tenant's service agreement.
    Returns None if all pass, or a violation string that BLOCKS the run.

    CONSTRAINTS CHECKED:
    ────────────────────
    1. reject_if_silver_table_missing
       Queries the silver Cassandra table. If it doesn't exist (e.g. was dropped
       for testing or never bootstrapped), the run is blocked.
       WHY: Running the pipeline without the target table would fail mid-run,
       wasting time and leaving dangling cache files.

    2. reject_if_pending_overflow
       Counts .jsonl files in the pending dir.
       If count >= max_cache_files_pending → blocked.
       WHY: Prevents unbounded disk usage if the transform stage is repeatedly
       failing (e.g. Cassandra silver table issues). Files pile up in pending.

    3. require_non_empty_bronze
       Counts rows in the bronze table. If zero → blocked.
       WHY: No source data = nothing to process. Saves running a pipeline
       that will produce 0 records.

    4. max_file_size_mb (data_limits)
       Checks if any existing pending file already exceeds the size limit.
       WHY: Large files slow the transform stage. If a file is already over
       the limit, the run would produce more oversized files.

    5. require_geo_fields (tenantB only)
       Samples 5 bronze rows and checks that geo.lat + geo.lon are present.
       WHY: TenantB's silver schema requires lat/lon. If all sampled records
       are missing geo fields, the whole run would load 0 useful records.
    """
    constraints  = cfg.get("constraints",  {})
    data_limits  = cfg.get("data_limits",  {})
    silver_table = cfg.get("silver_table", {})
    caching      = cfg.get("caching",      {})

    # ── 1. Silver table must exist ─────────────────────────────────────────────
    if constraints.get("reject_if_silver_table_missing"):
        ks = silver_table.get("keyspace", "")
        tb = silver_table.get("table",    "")
        if ks and _cassandra_ok:
            try:
                _session.execute(f"SELECT * FROM {ks}.{tb} LIMIT 1")
            except Exception:
                return f"CONSTRAINT_VIOLATION: Silver table {ks}.{tb} does not exist"

    # ── 2. Pending dir must not overflow ──────────────────────────────────────
    if constraints.get("reject_if_pending_overflow"):
        pending_dir = Path(caching.get("pending_dir", f"/app/cache/{tenant_id}/pending"))
        max_pending = int(data_limits.get("max_cache_files_pending", 20))
        if pending_dir.exists():
            count = len(list(pending_dir.glob("*.jsonl")))
            if count >= max_pending:
                return (f"CONSTRAINT_VIOLATION: {count} pending files >= max {max_pending}. "
                        "Clear pending dir before next run.")

    # ── 3. Bronze table must have at least 1 record ───────────────────────────
    if constraints.get("require_non_empty_bronze"):
        ks_bronze = f"{tenant_id}_bronze"
        try:
            cnt = _session.execute(
                f"SELECT COUNT(*) FROM {ks_bronze}.records"
            ).one()[0]
            if cnt == 0:
                return f"CONSTRAINT_VIOLATION: Bronze table {ks_bronze}.records is empty"
        except Exception:
            pass   # if we can't check (table missing), don't block the run

    # ── 4. No existing pending file exceeds max_file_size_mb ──────────────────
    if data_limits.get("max_file_size_mb"):
        max_bytes   = int(data_limits["max_file_size_mb"]) * 1024 * 1024
        pending_dir = Path(caching.get("pending_dir", f"/app/cache/{tenant_id}/pending"))
        if pending_dir.exists():
            for f in pending_dir.glob("*.jsonl"):
                size = f.stat().st_size
                if size > max_bytes:
                    return (f"CONSTRAINT_VIOLATION: Pending file {f.name} is "
                            f"{size // (1024*1024)}MB, exceeds max "
                            f"{data_limits['max_file_size_mb']}MB")

    # ── 5. TenantB: bronze records must contain geo lat/lon fields ────────────
    if constraints.get("require_geo_fields") and _cassandra_ok:
        ks_bronze = f"{tenant_id}_bronze"
        try:
            rows = list(_session.execute(
                f"SELECT payload FROM {ks_bronze}.records LIMIT 5"
            ))
            import json as _json
            missing = 0
            for row in rows:
                try:
                    obj = _json.loads(row.payload)
                    geo = obj.get("geo") or {}
                    if not geo.get("lat") or not geo.get("lon"):
                        missing += 1
                except Exception:
                    missing += 1
            # Only block if ALL sampled records have missing geo (not just some)
            if rows and missing == len(rows):
                return (f"CONSTRAINT_VIOLATION: require_geo_fields=true but sampled "
                        f"{missing}/{len(rows)} records have no geo.lat/geo.lon")
        except Exception:
            pass   # don't block if we can't sample

    return None   # all constraints passed → run is allowed


# ── pipeline runner ───────────────────────────────────────────────────────────

def run_pipeline(tenant_id: str, cfg: dict):
    """
    Core function: run one silver pipeline iteration for a tenant.
    Called by the scheduler (automatic) and by POST /tenants/{id}/run (manual).

    WHAT HAPPENS:
    ─────────────
    1. Check constraints — if any fail, log "constraint_violation" and return early
    2. Build the subprocess command from the service agreement config
    3. Launch the silverpipeline script as a child process
    4. Read stdout line by line (each line is a JSON log event)
    5. Find the "stage=complete" line and extract metrics
    6. Kill the process if it runs longer than max_runtime_seconds
    7. Persist the full run record to Cassandra

    BLACKBOX CONTRACT WITH SILVERPIPELINE:
    ───────────────────────────────────────
    The pipeline MUST:
      - Accept: --run-id, --max-records, --cassandra, --pending-dir, --processed-dir
      - Print JSON lines to stdout (each line = one stage update)
      - Print a final line with stage="complete" and status="ok" containing:
        run_id, elapsed_sec, extract_sec, transform_sec, data_size_bytes,
        cache_mode, records_loaded, errors
      - Exit 0 on success, exit 1 on failure

    The batchmanager doesn't care HOW the pipeline works internally.
    It only reads the "complete" line and the exit code.
    """
    run_id     = str(uuid.uuid4())[:8]   # short unique ID for this run
    t_start    = time.time()
    started_at = datetime.now(timezone.utc).isoformat()

    # Extract config values from the service agreement YAML
    pipeline_script  = cfg.get("pipeline_script", f"silverpipeline_{tenant_id}.py")
    data_limits      = cfg.get("data_limits",      {})
    caching          = cfg.get("caching",          {})
    scheduling       = cfg.get("scheduling",       {})

    max_records   = int(data_limits.get("max_records_per_run", 5000))
    max_runtime   = int(scheduling.get("max_runtime_seconds",  120))
    pending_dir   = caching.get("pending_dir",   f"/app/cache/{tenant_id}/pending")
    processed_dir = caching.get("processed_dir", f"/app/cache/{tenant_id}/processed")

    print(f"\n==> [{tenant_id}] Run {run_id} starting — script={pipeline_script}")

    # ── Step 1: Constraint check ───────────────────────────────────────────────
    violation = check_constraints(tenant_id, cfg)
    if violation:
        elapsed = round(time.time() - t_start, 3)
        print(f"🚫 [{tenant_id}] Run {run_id} BLOCKED: {violation}")
        # Still log the blocked run to Cassandra so it appears in /stats
        persist_log(
            tenant_id, run_id, started_at, datetime.now(timezone.utc).isoformat(),
            "constraint_violation", 0, 0, elapsed,
            0.0, 0.0, 0, "none",
            pipeline_script, violation,
        )
        return

    # ── Step 2: Build subprocess command ──────────────────────────────────────
    cmd = [
        "python", pipeline_script,
        "--run-id",        run_id,
        "--max-records",   str(max_records),
        "--cassandra",     CASSANDRA_HOST,
        "--pending-dir",   pending_dir,
        "--processed-dir", processed_dir,
    ]

    # Environment for the subprocess — inherits all batchmanager env vars
    # GCS_BUCKET is inherited automatically if set in docker-compose.yaml
    env = {
        **os.environ,
        "CASSANDRA_HOST":       CASSANDRA_HOST,
        "CACHE_PENDING_DIR":    pending_dir,
        "CACHE_PROCESSED_DIR":  processed_dir,
        "MAX_RECORDS":          str(max_records),
    }

    # ── Step 3: Metrics placeholders (filled from pipeline stdout) ────────────
    stdout_lines:   List[str] = []
    status          = "unknown"
    records_loaded  = 0
    errors          = 0
    extract_sec     = 0.0
    transform_sec   = 0.0
    data_size_bytes = 0
    cache_mode      = "local"

    # ── Step 4: Launch subprocess and capture output ──────────────────────────
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge stderr into stdout (capture everything)
            text=True,
            env=env,
            cwd="/app",                # pipeline scripts are in /app inside the container
        )
        try:
            # Wait for the pipeline to finish (or timeout)
            stdout, _ = proc.communicate(timeout=max_runtime)
            stdout_lines = stdout.splitlines()
        except subprocess.TimeoutExpired:
            # Kill the subprocess if it runs too long
            proc.kill()
            proc.communicate()
            msg = f"Pipeline timed out after {max_runtime}s"
            print(f"⏱ [{tenant_id}] Run {run_id}: {msg}")
            persist_log(
                tenant_id, run_id, started_at, datetime.now(timezone.utc).isoformat(),
                "timeout", 0, 0, round(time.time() - t_start, 3),
                0.0, 0.0, 0, "local",
                pipeline_script, msg,
            )
            return

        exit_code = proc.returncode

        # ── Step 5: Parse "complete" line from stdout ──────────────────────────
        # The pipeline prints one JSON line per stage. We scan for the "complete"
        # line which carries all the metrics we need to persist.
        for line in stdout_lines:
            print(f"  [{tenant_id}] {line}")    # echo pipeline output to batchmanager logs
            try:
                obj = json.loads(line)
                if obj.get("stage") == "complete" and obj.get("status") == "ok":
                    # Extract all performance metrics from the complete line
                    records_loaded  = obj.get("records_loaded",  0)
                    errors          = obj.get("errors",          0)
                    extract_sec     = float(obj.get("extract_sec",     0.0))
                    transform_sec   = float(obj.get("transform_sec",   0.0))
                    data_size_bytes = int(obj.get("data_size_bytes",   0))
                    cache_mode      = obj.get("cache_mode",            "local")
                elif obj.get("status") == "error":
                    errors += 1   # count stage-level errors too
            except Exception:
                pass   # non-JSON lines (e.g. Python tracebacks) are ignored

        status = "success" if exit_code == 0 else "failed"

    except FileNotFoundError:
        # The pipeline script doesn't exist in /app
        status       = "failed"
        stdout_lines = [f"Pipeline script not found: {pipeline_script}"]
        print(f"❌ [{tenant_id}] {stdout_lines[0]}")
    except Exception as e:
        status       = "failed"
        stdout_lines = [f"Unexpected error launching pipeline: {e}"]
        print(f"❌ [{tenant_id}] Run {run_id}: {e}")

    # ── Step 6: Persist full run record to Cassandra ──────────────────────────
    elapsed     = round(time.time() - t_start, 3)
    finished_at = datetime.now(timezone.utc).isoformat()
    # Store last 50 lines of stdout as audit trail (captures all stage logs)
    detail      = "\n".join(stdout_lines[-50:])

    print(f"✅ [{tenant_id}] Run {run_id}: status={status} "
          f"records={records_loaded} errors={errors} "
          f"extract={extract_sec}s transform={transform_sec}s "
          f"size={data_size_bytes}B cache={cache_mode} total={elapsed}s")

    persist_log(
        tenant_id, run_id, started_at, finished_at,
        status, records_loaded, errors, elapsed,
        extract_sec, transform_sec, data_size_bytes, cache_mode,
        pipeline_script, detail,
    )


# ── APScheduler setup ─────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()

def setup_schedules(configs: Dict[str, dict]):
    """
    Register one periodic job per tenant using APScheduler.
    Each job calls run_pipeline(tenant_id, cfg) every interval_minutes.

    WHY 30s DELAY ON FIRST RUN?
    We add a 30s delay to the first scheduled run so that Cassandra schema
    (created by bootstrap.sh which runs after docker compose up) is fully
    ready before the first automatic pipeline fires.
    Manual /run calls via the API are NOT affected by this delay.
    """
    import datetime as _dt
    first_run = datetime.now(timezone.utc) + _dt.timedelta(seconds=30)

    for tenant_id, cfg in configs.items():
        interval = int(cfg.get("scheduling", {}).get("interval_minutes", 5))
        scheduler.add_job(
            run_pipeline,
            "interval",
            minutes=interval,
            start_date=first_run,          # first run 30s after startup
            args=[tenant_id, cfg],
            id=f"silver_{tenant_id}",
            replace_existing=True,
        )
        print(f"📅 Scheduled [{tenant_id}] silverpipeline every {interval}min "
              f"(first automatic run in 30s)")


# ── FastAPI app + lifecycle ───────────────────────────────────────────────────

TENANT_CONFIGS_MAP: Dict[str, dict] = {}   # populated at startup

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup sequence:
      1. Connect to Cassandra (blocking, with retry)
      2. Load tenant config YAMLs
      3. Register scheduled jobs
      4. Start the APScheduler background thread
    """
    global TENANT_CONFIGS_MAP
    _connect_cassandra()
    TENANT_CONFIGS_MAP = load_tenant_configs()
    setup_schedules(TENANT_CONFIGS_MAP)
    scheduler.start()
    print("✅ batchmanager scheduler started")
    yield
    scheduler.shutdown()

app = FastAPI(title="mysimbdp-batchmanager", lifespan=lifespan)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """
    Readiness check.
    cassandra_ready=false means the batchmanager can't log pipeline runs.
    Check batchmanager logs for the real error (usually a missing schema column).
    """
    return {
        "ok":               True,
        "cassandra_ready":  _cassandra_ok,
        "tenants":          list(TENANT_CONFIGS_MAP.keys()),
    }


@app.get("/tenants")
def list_tenants():
    """List all registered tenants with their pipeline script and schedule."""
    return {
        tid: {
            "pipeline_script":  cfg.get("pipeline_script"),
            "interval_minutes": cfg.get("scheduling", {}).get("interval_minutes"),
            "silver_table":     cfg.get("silver_table"),
        }
        for tid, cfg in TENANT_CONFIGS_MAP.items()
    }


@app.post("/tenants/{tenant_id}/run")
def trigger_run(tenant_id: str):
    """
    Manually trigger a pipeline run for a tenant.
    Runs in a background thread so the HTTP response returns immediately.
    Use GET /tenants/{id}/logs to check the result after a few seconds.
    """
    if tenant_id not in TENANT_CONFIGS_MAP:
        raise HTTPException(404, detail=f"Unknown tenant: {tenant_id}")
    import threading
    t = threading.Thread(
        target=run_pipeline,
        args=[tenant_id, TENANT_CONFIGS_MAP[tenant_id]],
        daemon=True,
    )
    t.start()
    return {"ok": True, "tenant": tenant_id, "message": "Pipeline triggered in background"}


@app.get("/tenants/{tenant_id}/logs")
def get_logs(tenant_id: str, limit: int = 20):
    """
    Fetch recent pipeline run records for a tenant from Cassandra.
    Ordered by started_at DESC (newest first).
    Includes: run_id, status, records_loaded, errors, all timing + cache metrics.
    """
    if not _cassandra_ok:
        raise HTTPException(503, detail="Cassandra not ready")
    try:
        rows = _session.execute(
            f"""SELECT run_id, started_at, finished_at, status,
                       records_loaded, errors, elapsed_sec,
                       extract_sec, transform_sec, data_size_bytes, cache_mode,
                       pipeline_script
                FROM platform_logs.silver_pipeline_logs
                WHERE tenant_id = '{tenant_id}'
                ORDER BY started_at DESC
                LIMIT {limit}"""
        )
        return {"tenant": tenant_id, "logs": [dict(r._asdict()) for r in rows]}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get("/stats")
def platform_stats():
    """
    Aggregate platform-wide statistics from silver_pipeline_logs.

    Returns:
      per_tenant: for each tenant, runs broken down by status,
                  with avg timings and total records/bytes
      platform:   overall totals + averages across all tenants

    WHY AGGREGATE IN PYTHON?
    Cassandra GROUP BY only works on partition key columns.
    silver_pipeline_logs has tenant_id as the partition key.
    We can group by tenant_id (and do), but we can't GROUP BY status in CQL.
    So we fetch all rows and aggregate in Python using defaultdict.
    """
    if not _cassandra_ok:
        raise HTTPException(503, detail="Cassandra not ready")

    # Platform-wide accumulators
    platform_totals = {
        "total_runs": 0, "success": 0, "failed": 0, "constraint_violation": 0,
        "total_records": 0, "total_errors": 0, "total_bytes": 0,
        "avg_elapsed_sec": 0.0, "avg_extract_sec": 0.0, "avg_transform_sec": 0.0,
        "cache_modes": defaultdict(int),
    }
    result = {}

    for tenant_id in TENANT_CONFIGS_MAP:
        try:
            rows = list(_session.execute(
                f"""SELECT status, records_loaded, errors, elapsed_sec,
                           extract_sec, transform_sec, data_size_bytes, cache_mode
                    FROM platform_logs.silver_pipeline_logs
                    WHERE tenant_id = '{tenant_id}'
                    LIMIT 500 ALLOW FILTERING"""
            ))
        except Exception as e:
            result[tenant_id] = {"error": str(e)}
            continue

        # Per-tenant aggregation bucketed by status
        buckets      = defaultdict(lambda: {
            "count": 0, "total_records": 0, "total_errors": 0,
            "total_bytes": 0, "sum_elapsed": 0.0,
            "sum_extract": 0.0, "sum_transform": 0.0,
        })
        cache_counts = defaultdict(int)

        for r in rows:
            b = buckets[r.status or "unknown"]
            b["count"]          += 1
            b["total_records"]  += (r.records_loaded or 0)
            b["total_errors"]   += (r.errors         or 0)
            b["total_bytes"]    += (r.data_size_bytes or 0)
            b["sum_elapsed"]    += (r.elapsed_sec     or 0.0)
            b["sum_extract"]    += (r.extract_sec     or 0.0)
            b["sum_transform"]  += (r.transform_sec   or 0.0)
            cache_counts[r.cache_mode or "local"] += 1

            # Accumulate into platform totals
            platform_totals["total_runs"]    += 1
            platform_totals["total_records"] += (r.records_loaded or 0)
            platform_totals["total_errors"]  += (r.errors         or 0)
            platform_totals["total_bytes"]   += (r.data_size_bytes or 0)
            platform_totals["avg_elapsed_sec"]   += (r.elapsed_sec  or 0.0)
            platform_totals["avg_extract_sec"]   += (r.extract_sec  or 0.0)
            platform_totals["avg_transform_sec"] += (r.transform_sec or 0.0)
            platform_totals[r.status or "unknown"] = \
                platform_totals.get(r.status or "unknown", 0) + 1
            platform_totals["cache_modes"][r.cache_mode or "local"] += 1

        # Compute per-status averages for this tenant
        tenant_summary = {}
        for status, b in buckets.items():
            n = b["count"]
            tenant_summary[status] = {
                "runs":              n,
                "total_records":     b["total_records"],
                "total_errors":      b["total_errors"],
                "total_bytes":       b["total_bytes"],
                "avg_elapsed_sec":   round(b["sum_elapsed"]   / n, 3) if n else 0,
                "avg_extract_sec":   round(b["sum_extract"]   / n, 3) if n else 0,
                "avg_transform_sec": round(b["sum_transform"] / n, 3) if n else 0,
            }
        result[tenant_id] = {
            "by_status":   tenant_summary,
            "cache_modes": dict(cache_counts),
        }

    # Finalize platform-wide averages (divide sums by total run count)
    n = platform_totals["total_runs"]
    if n:
        platform_totals["avg_elapsed_sec"]   = round(platform_totals["avg_elapsed_sec"]   / n, 3)
        platform_totals["avg_extract_sec"]   = round(platform_totals["avg_extract_sec"]   / n, 3)
        platform_totals["avg_transform_sec"] = round(platform_totals["avg_transform_sec"] / n, 3)
    platform_totals["cache_modes"] = dict(platform_totals["cache_modes"])

    return {"per_tenant": result, "platform": platform_totals}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
