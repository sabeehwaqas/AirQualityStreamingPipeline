#!/usr/bin/env python3
"""
silverpipeline_tenantA.py  —  mysimbdp Silver Pipeline (TenantA)
=================================================================

WHAT THIS FILE DOES
-------------------
This is the silver transformation pipeline for TenantA.
It runs as a SHORT-LIVED subprocess, launched by batchmanager.py.

It has two stages:
  STAGE 1 - EXTRACT:
    Read new bronze records from Cassandra (using a watermark for CDC),
    write them to a local .jsonl cache file,
    optionally upload the cache file to GCS.

  STAGE 2 - TRANSFORM + LOAD:
    Read the .jsonl cache file,
    transform each record (cast types, derive aqi_bucket),
    insert into the silver Cassandra table,
    move cache file from pending/ to processed/.

BLACKBOX CONTRACT WITH BATCHMANAGER
-------------------------------------
  Invoked as:
    python silverpipeline_tenantA.py --run-id <id> --max-records N
                                     --cassandra HOST --pending-dir PATH
                                     --processed-dir PATH

  Exit 0 = success | Exit 1 = failure

  Every status update is printed as a single JSON line to stdout.
  The batchmanager reads the line where stage="complete" and status="ok"
  to extract performance metrics. ALL fields below must be present:
    run_id, elapsed_sec, extract_sec, transform_sec,
    data_size_bytes, cache_mode, records_loaded, errors

WATERMARK / CDC MECHANISM
--------------------------
  The pipeline reads platform_logs.silver_watermarks WHERE tenant_id='tenantA'.
  The watermark is the ingest_ts of the LAST bronze record processed.
  On each run: SELECT ... WHERE ingest_ts > watermark LIMIT max_records
  After successful load: UPDATE watermark to max(ingest_ts) of this batch.
  This ensures each bronze record is processed EXACTLY ONCE.

GCS CACHING (DUAL-MODE)
------------------------
  If GCS_BUCKET env var is set:
    cache_mode = "local+gcs"
    Stage 1: write local .jsonl AND upload to GCS pending/
    Stage 2: transform from local file, move GCS file pending/ → processed/

  If GCS_BUCKET is not set:
    cache_mode = "local"
    Stage 1: write local .jsonl only
    Stage 2: transform from local file only

  GCS failures are NON-FATAL — the pipeline falls back to local-only mode
  rather than crashing. The data is never lost.

TENANTA SPECIFIC TRANSFORMATIONS
---------------------------------
  - Cast pm10_P1, pm2_5_P2, lat, lon, alt to float
  - Derive aqi_bucket from pm2_5_P2:
      good              ≤ 12 μg/m³
      moderate          ≤ 35 μg/m³
      unhealthy_sensitive ≤ 55 μg/m³
      unhealthy         > 55 μg/m³
      unknown           if pm2_5 is None
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cassandra.cluster import Cluster

# ── GCS setup (optional) ──────────────────────────────────────────────────────
#
# GCS_BUCKET is set via docker-compose.yaml (batchmanager environment section).
# If empty/unset → local-only mode (no GCS operations).
# If set → dual-mode: local + GCS upload/move.
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()    # bucket name only, no gs:// prefix
GCS_PREFIX = os.getenv("GCS_PREFIX", "tenantA")     # folder path inside the bucket
USE_GCS    = bool(GCS_BUCKET)

if USE_GCS:
    try:
        from google.cloud import storage as _gcs
        _gcs_client = _gcs.Client()   # uses GOOGLE_APPLICATION_CREDENTIALS env var
    except Exception as _e:
        # GCS library not installed or key file missing — fall back gracefully
        print(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "pipeline": "silverpipeline_tenantA", "stage": "init",
            "status": "warning",
            "detail": f"GCS unavailable ({_e}); falling back to local-only",
        }), flush=True)
        USE_GCS = False


# ── logging helper ────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log_event(stage: str, status: str, detail: dict = None):
    """
    Print one JSON log line to stdout.
    batchmanager reads these lines to track pipeline progress.
    The 'complete' line is parsed for metrics persistence.
    """
    print(json.dumps({
        "ts":       now_iso(),
        "pipeline": "silverpipeline_tenantA",
        "stage":    stage,
        "status":   status,
        **(detail or {}),
    }), flush=True)   # flush=True ensures batchmanager sees lines in real time


# ── GCS helper functions ──────────────────────────────────────────────────────

def gcs_upload_file(local_path: Path, subfolder: str) -> str:
    """
    Upload a local file to GCS under GCS_PREFIX/subfolder/filename.
    Returns the gs:// URI of the uploaded file.
    Raises on error — caller should catch and handle gracefully.
    """
    bucket    = _gcs_client.bucket(GCS_BUCKET)
    blob_name = f"{GCS_PREFIX}/{subfolder}/{local_path.name}"
    bucket.blob(blob_name).upload_from_filename(str(local_path))
    return f"gs://{GCS_BUCKET}/{blob_name}"

def gcs_delete_blob(blob_name: str):
    """Delete a GCS blob if it exists. Used to 'move' files (upload new + delete old)."""
    bucket = _gcs_client.bucket(GCS_BUCKET)
    b      = bucket.blob(blob_name)
    if b.exists():
        b.delete()


# ── AQI bucket derivation ─────────────────────────────────────────────────────

def pm25_bucket(val: Optional[float]) -> str:
    """
    Derive an AQI quality bucket from PM2.5 concentration (μg/m³).
    Based on US EPA AQI breakpoints (simplified).

    good               = PM2.5 ≤ 12    (safe air quality)
    moderate           = PM2.5 ≤ 35    (acceptable)
    unhealthy_sensitive = PM2.5 ≤ 55  (unhealthy for sensitive groups)
    unhealthy          = PM2.5 > 55    (unhealthy for everyone)
    unknown            = no data
    """
    if val is None: return "unknown"
    if val <= 12:   return "good"
    if val <= 35:   return "moderate"
    if val <= 55:   return "unhealthy_sensitive"
    return "unhealthy"


def safe_float(x) -> Optional[float]:
    """Cast to float, return None if not convertible (handles strings, None, empty)."""
    try:    return float(x)
    except: return None


def transform_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply TenantA silver transformations to a single raw record.

    Input: raw dict from the .jsonl cache file (extracted from bronze payload)
    Output: dict ready to INSERT into tenantA_silver.air_quality

    Transformations:
      - All numeric fields cast to float (handles string values from the source API)
      - aqi_bucket derived from pm2_5_P2
      - silver_ts set to current UTC time (platform ingestion timestamp)
    """
    pm25 = safe_float(raw.get("pm2_5_P2"))
    pm10 = safe_float(raw.get("pm10_P1"))
    return {
        "sensor_id":   raw.get("sensor_id", "unknown"),
        "event_ts":    raw.get("event_ts"),
        "event_id":    raw.get("event_id", 0),
        "ingest_ts":   raw.get("ingest_ts"),
        "lat":         safe_float(raw.get("lat")),
        "lon":         safe_float(raw.get("lon")),
        "alt":         safe_float(raw.get("alt")),
        "country":     raw.get("country"),
        "sensor_type": raw.get("sensor_type"),
        "pm10":        pm10,
        "pm2_5":       pm25,
        "aqi_bucket":  pm25_bucket(pm25),   # derived field
        "silver_ts":   now_iso(),            # platform timestamp for silver record
    }


# ── watermark helpers ─────────────────────────────────────────────────────────

def get_watermark(session) -> Optional[str]:
    """
    Read the last-processed ingest_ts from silver_watermarks.
    Returns None if no watermark exists (first run — process all bronze records).
    """
    try:
        row = session.execute(
            "SELECT last_processed_ts FROM platform_logs.silver_watermarks "
            "WHERE tenant_id = 'tenantA'"
        ).one()
        return str(row.last_processed_ts) if row else None
    except Exception:
        return None   # treat as first run

def set_watermark(session, ts: str):
    """Update the watermark to ts (the max ingest_ts processed in this run)."""
    session.execute(
        "INSERT INTO platform_logs.silver_watermarks "
        "(tenant_id, last_processed_ts) VALUES ('tenantA', %s)", (ts,)
    )


# ── STAGE 1: Extract bronze → local .jsonl cache ──────────────────────────────

def extract_to_cache(
    session,
    pending_dir: Path,
    run_id: str,
    max_records: int,
    watermark_ts: Optional[str],
) -> Tuple[List[Path], int, str]:
    """
    Read new bronze records from Cassandra and write to a local .jsonl file.
    Optionally uploads to GCS if GCS_BUCKET is set.

    WATERMARK LOGIC (CDC — Change Data Capture):
      - If watermark exists: SELECT WHERE ingest_ts > watermark (only new records)
      - If no watermark: SELECT all records up to max_records (first run)
      - ALLOW FILTERING is needed because ingest_ts is a clustering column, not PK

    Returns:
      list_of_files:    [Path] to the local .jsonl file(s) written
      total_bytes:      total bytes written to the file
      cache_mode:       "local" or "local+gcs"
    """
    pending_dir.mkdir(parents=True, exist_ok=True)

    # ── Read from bronze Cassandra ─────────────────────────────────────────────
    if watermark_ts:
        # CDC mode: only fetch records newer than the last processed timestamp
        rows = session.execute(
            """SELECT sensor_id, ingest_ts, event_ts, event_id, payload
               FROM tenantA_bronze.records
               WHERE ingest_ts > %s LIMIT %s ALLOW FILTERING""",
            (watermark_ts, max_records),
        )
    else:
        # First run — no watermark, fetch up to max_records
        rows = session.execute(
            f"SELECT sensor_id, ingest_ts, event_ts, event_id, payload "
            f"FROM tenantA_bronze.records LIMIT {max_records}"
        )

    records = list(rows)
    if not records:
        return [], 0, "local"   # nothing to process this run

    # ── Write to local .jsonl cache file ──────────────────────────────────────
    # One JSON object per line (JSON Lines format).
    # We extract the fields from both the Cassandra columns AND the raw payload JSON.
    # The payload JSON contains the full message the producer originally sent to Kafka.
    cache_file  = pending_dir / f"tenantA_{run_id}.jsonl"
    total_bytes = 0

    with open(cache_file, "w") as fh:
        for row in records:
            try:
                payload_obj = json.loads(row.payload)   # parse the stored raw JSON
            except Exception:
                payload_obj = {}   # malformed payload — use empty dict (still ingest)
            rec = {
                "sensor_id":   row.sensor_id,
                "ingest_ts":   str(row.ingest_ts),
                "event_ts":    row.event_ts,
                "event_id":    row.event_id,
                # Fields from the original producer payload (flat structure for tenantA)
                "lat":         payload_obj.get("lat"),
                "lon":         payload_obj.get("lon"),
                "alt":         payload_obj.get("alt"),
                "country":     payload_obj.get("country"),
                "sensor_type": payload_obj.get("sensor_type"),
                "pm10_P1":     payload_obj.get("pm10_P1"),
                "pm2_5_P2":    payload_obj.get("pm2_5_P2"),
            }
            line = json.dumps(rec) + "\n"
            fh.write(line)
            total_bytes += len(line.encode("utf-8"))

    cache_mode = "local"

    # ── Optionally upload to GCS ───────────────────────────────────────────────
    # GCS failure is NON-FATAL. We log a warning and continue with local-only.
    # The transform stage reads from the local file regardless of GCS status.
    if USE_GCS:
        try:
            t0      = time.time()
            gcs_uri = gcs_upload_file(cache_file, "pending")
            log_event("extract", "gcs_upload_ok", {
                "gcs_uri":        gcs_uri,
                "bytes":          total_bytes,
                "gcs_upload_sec": round(time.time() - t0, 3),
            })
            cache_mode = "local+gcs"
        except Exception as e:
            log_event("extract", "gcs_upload_failed", {"error": str(e)})
            # Continue with local-only — transform stage is unaffected

    return [cache_file], total_bytes, cache_mode


# ── STAGE 2: Transform + Load .jsonl → silver Cassandra ───────────────────────

def transform_and_load(
    session,
    cache_files: List[Path],
    processed_dir: Path,
) -> Dict[str, Any]:
    """
    Read .jsonl cache files, transform each record, insert to silver Cassandra,
    then move the file from pending/ to processed/ (local + GCS).

    Returns dict with: records_loaded, errors, transform_sec, max_ingest_ts
    max_ingest_ts is used to update the watermark after a successful run.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Prepare parameterized INSERT for silver table
    # Column order must match the VALUES placeholders exactly
    prepared = session.prepare(
        """INSERT INTO tenantA_silver.air_quality
           (sensor_id, silver_ts, event_ts, event_id, ingest_ts,
            lat, lon, alt, country, sensor_type, pm10, pm2_5, aqi_bucket)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    total_records = 0
    total_errors  = 0
    max_ingest_ts = None   # track max so we can advance the watermark
    t_start       = time.time()

    for cache_file in cache_files:
        with open(cache_file) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw    = json.loads(line)
                    silver = transform_record(raw)   # apply transformations
                    session.execute(prepared, (
                        silver["sensor_id"],  silver["silver_ts"],
                        silver["event_ts"],   int(silver["event_id"] or 0),
                        silver["ingest_ts"],  silver["lat"],
                        silver["lon"],        silver["alt"],
                        silver["country"],    silver["sensor_type"],
                        silver["pm10"],       silver["pm2_5"],
                        silver["aqi_bucket"],
                    ))
                    total_records += 1
                    # Track max ingest_ts to advance watermark
                    its = raw.get("ingest_ts")
                    if its and (max_ingest_ts is None or its > max_ingest_ts):
                        max_ingest_ts = its
                except Exception as e:
                    total_errors += 1
                    log_event("transform_load", "record_error",
                              {"error": str(e), "line": line[:120]})

        # ── Move cache file: pending/ → processed/ ─────────────────────────────
        dest = processed_dir / cache_file.name
        cache_file.rename(dest)   # atomic rename on same filesystem

        # ── Also move in GCS: upload processed copy, delete pending blob ───────
        if USE_GCS:
            try:
                gcs_upload_file(dest, "processed")                           # upload to processed/
                gcs_delete_blob(f"{GCS_PREFIX}/pending/{cache_file.name}")   # delete from pending/
                log_event("transform_load", "gcs_move_ok", {"file": cache_file.name})
            except Exception as e:
                log_event("transform_load", "gcs_move_failed", {"error": str(e)})

    return {
        "records_loaded": total_records,
        "errors":         total_errors,
        "transform_sec":  round(time.time() - t_start, 3),
        "max_ingest_ts":  max_ingest_ts,
    }


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TenantA Silver Pipeline")
    parser.add_argument("--run-id",        default=str(uuid.uuid4())[:8])
    parser.add_argument("--max-records",   type=int,
                        default=int(os.getenv("MAX_RECORDS", "5000")))
    parser.add_argument("--cassandra",     default=os.getenv("CASSANDRA_HOST", "cassandra"))
    parser.add_argument("--pending-dir",
                        default=os.getenv("CACHE_PENDING_DIR",   "/app/cache/tenantA/pending"))
    parser.add_argument("--processed-dir",
                        default=os.getenv("CACHE_PROCESSED_DIR", "/app/cache/tenantA/processed"))
    args = parser.parse_args()

    run_id        = args.run_id
    pending_dir   = Path(args.pending_dir)
    processed_dir = Path(args.processed_dir)
    t_total       = time.time()

    # ── START log line — batchmanager reads this to confirm pipeline started ───
    log_event("start", "ok", {
        "run_id":      run_id,
        "max_records": args.max_records,
        "gcs_enabled": USE_GCS,
        "gcs_bucket":  GCS_BUCKET or None,
    })

    # ── Cassandra connection with retry ───────────────────────────────────────
    # Retry up to 10 times — this subprocess may start before Cassandra is ready.
    # Backoff: 3s, 6s, 9s, ... 27s between attempts.
    cluster = None
    session = None
    for _attempt in range(1, 11):
        try:
            cluster = Cluster([args.cassandra])
            session = cluster.connect()
            log_event("cassandra_connect", "ok", {"attempt": _attempt})
            break
        except Exception as e:
            log_event("cassandra_connect", "retry", {"attempt": _attempt, "error": str(e)})
            if _attempt == 10:
                log_event("cassandra_connect", "error", {"error": str(e)})
                sys.exit(1)
            time.sleep(3 * _attempt)   # increasing backoff

    # ── STAGE 1: Extract ──────────────────────────────────────────────────────
    try:
        watermark = get_watermark(session)
        log_event("extract", "start", {"watermark": watermark})
        t_ex = time.time()

        cache_files, data_size_bytes, cache_mode = extract_to_cache(
            session, pending_dir, run_id, args.max_records, watermark
        )
        extract_sec = round(time.time() - t_ex, 3)

        log_event("extract", "ok", {
            "files_written":   len(cache_files),
            "data_size_bytes": data_size_bytes,
            "extract_sec":     extract_sec,
            "cache_mode":      cache_mode,
        })
    except Exception as e:
        log_event("extract", "error", {"error": str(e)})
        cluster.shutdown()
        sys.exit(1)

    # No new bronze records since last watermark — exit cleanly
    if not cache_files:
        log_event("extract", "no_new_data", {"watermark": watermark})
        cluster.shutdown()
        sys.exit(0)

    # ── STAGE 2: Transform + Load ─────────────────────────────────────────────
    try:
        log_event("transform_load", "start", {"files": len(cache_files)})
        result = transform_and_load(session, cache_files, processed_dir)
        log_event("transform_load", "ok", result)

        # Advance watermark so next run only picks up NEW records
        if result["max_ingest_ts"]:
            set_watermark(session, result["max_ingest_ts"])
            log_event("watermark", "updated", {"new_watermark": result["max_ingest_ts"]})
    except Exception as e:
        log_event("transform_load", "error", {"error": str(e)})
        cluster.shutdown()
        sys.exit(1)

    # ── COMPLETE line — batchmanager MUST find this to record metrics ─────────
    # All 8 fields are required. batchmanager parses this exact JSON line.
    elapsed_sec = round(time.time() - t_total, 3)
    log_event("complete", "ok", {
        "run_id":          run_id,
        "elapsed_sec":     elapsed_sec,          # total pipeline wall time
        "extract_sec":     extract_sec,          # stage 1 time (includes GCS upload)
        "transform_sec":   result["transform_sec"],  # stage 2 time
        "data_size_bytes": data_size_bytes,      # .jsonl file size in bytes
        "cache_mode":      cache_mode,           # "local" or "local+gcs"
        "records_loaded":  result["records_loaded"],
        "errors":          result["errors"],
    })

    cluster.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    main()
