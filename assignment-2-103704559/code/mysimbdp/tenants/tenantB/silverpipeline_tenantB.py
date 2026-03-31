#!/usr/bin/env python3
"""
silverpipeline_tenantB.py  —  mysimbdp Silver Pipeline (TenantB)
=================================================================

WHAT THIS FILE DOES
-------------------
Same two-stage pipeline as tenantA, but for TenantB's NESTED schema.

TenantB differences from TenantA:
  - Bronze PK is location_id (not sensor_id)
  - Bronze payload has nested geo/device/measurements sub-objects
  - Silver table is tenantB_silver.geo_measurements
  - Derived field: has_pm_data (boolean) instead of aqi_bucket
  - Extra constraint in service agreement: require_geo_fields=true

STAGE 1 - EXTRACT:
  Read tenantB_bronze.records (with watermark CDC),
  flatten nested payload → .jsonl cache file,
  optionally upload to GCS.

STAGE 2 - TRANSFORM + LOAD:
  Read .jsonl, transform (cast coordinates, derive has_pm_data),
  insert to tenantB_silver.geo_measurements,
  move cache file pending/ → processed/.

TENANTB PAYLOAD STRUCTURE (what the producer sends):
------------------------------------------------------
  {
    "geo": {
      "location_id": 789,
      "lat": 60.1699,   "lon": 24.9384,   "alt": 10.0,
      "country": "FI",  "indoor": 0
    },
    "device": {
      "sensor_id": 123,
      "type": {"name": "SDS011", "manufacturer": "Nova Fitness"}
    },
    "measurements": {
      "P1": "12.3",    ← pm10 (μg/m³)
      "P2": "5.6"      ← pm2.5 (μg/m³)
    },
    "meta": {
      "event_id": 456, "timestamp_str": "2026-03-10 23:00:00"
    }
  }

BLACKBOX CONTRACT: same as tenantA — see silverpipeline_tenantA.py header.
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
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX = os.getenv("GCS_PREFIX", "tenantB")   # separate GCS folder for tenantB
USE_GCS    = bool(GCS_BUCKET)

if USE_GCS:
    try:
        from google.cloud import storage as _gcs
        _gcs_client = _gcs.Client()
    except Exception as _e:
        print(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "pipeline": "silverpipeline_tenantB", "stage": "init",
            "status": "warning",
            "detail": f"GCS unavailable ({_e}); falling back to local-only",
        }), flush=True)
        USE_GCS = False


# ── logging helper ────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log_event(stage: str, status: str, detail: dict = None):
    """Print one structured JSON log line to stdout. batchmanager reads these."""
    print(json.dumps({
        "ts":       now_iso(),
        "pipeline": "silverpipeline_tenantB",
        "stage":    stage,
        "status":   status,
        **(detail or {}),
    }), flush=True)


# ── GCS helpers ───────────────────────────────────────────────────────────────

def gcs_upload_file(local_path: Path, subfolder: str) -> str:
    """Upload local file to gs://GCS_BUCKET/GCS_PREFIX/subfolder/filename."""
    bucket    = _gcs_client.bucket(GCS_BUCKET)
    blob_name = f"{GCS_PREFIX}/{subfolder}/{local_path.name}"
    bucket.blob(blob_name).upload_from_filename(str(local_path))
    return f"gs://{GCS_BUCKET}/{blob_name}"

def gcs_delete_blob(blob_name: str):
    """Delete a GCS blob if it exists."""
    bucket = _gcs_client.bucket(GCS_BUCKET)
    b      = bucket.blob(blob_name)
    if b.exists():
        b.delete()


# ── transformations ───────────────────────────────────────────────────────────

def safe_float(x) -> Optional[float]:
    """Cast to float safely, return None on failure."""
    try:    return float(x)
    except: return None

def has_pm_data(measurements: dict) -> bool:
    """
    TenantB derived field: True if the measurements dict has any PM data.
    P1 = PM10 reading, P2 = PM2.5 reading.
    Used to flag records that are useful for air quality analysis.
    """
    return bool(measurements.get("P1") or measurements.get("P2"))

def transform_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply TenantB silver transformations to a single raw record.

    Input: row from the .jsonl cache (already flattened from bronze payload)
    Output: dict ready to INSERT into tenantB_silver.geo_measurements

    Transformations:
      - Cast lat, lon, alt to float (they come as strings from the API)
      - Cast pm10, pm2_5 to float from measurements dict
      - Derive has_pm_data boolean flag
      - Set silver_ts to current UTC time
    """
    measurements = raw.get("measurements") or {}
    return {
        "location_id": raw.get("location_id", "unknown"),
        "event_ts":    raw.get("event_ts"),
        "event_id":    raw.get("event_id", 0),
        "ingest_ts":   raw.get("ingest_ts"),
        "lat":         safe_float(raw.get("lat")),         # cast string → float
        "lon":         safe_float(raw.get("lon")),
        "alt":         safe_float(raw.get("alt")),
        "country":     raw.get("country"),
        "sensor_id":   raw.get("sensor_id"),
        "pm10":        safe_float(measurements.get("P1")),  # from nested measurements
        "pm2_5":       safe_float(measurements.get("P2")),
        "has_pm_data": has_pm_data(measurements),           # derived boolean field
        "silver_ts":   now_iso(),
    }


# ── watermark helpers ─────────────────────────────────────────────────────────

def get_watermark(session) -> Optional[str]:
    """Read last-processed ingest_ts for tenantB. Returns None on first run."""
    try:
        row = session.execute(
            "SELECT last_processed_ts FROM platform_logs.silver_watermarks "
            "WHERE tenant_id = 'tenantB'"
        ).one()
        return str(row.last_processed_ts) if row else None
    except Exception:
        return None

def set_watermark(session, ts: str):
    """Advance the tenantB watermark to ts (max ingest_ts of this batch)."""
    session.execute(
        "INSERT INTO platform_logs.silver_watermarks "
        "(tenant_id, last_processed_ts) VALUES ('tenantB', %s)", (ts,)
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
    Read new tenantB bronze records and write to a local .jsonl cache file.

    KEY DIFFERENCE FROM TENANTA:
    The bronze payload is NESTED (geo/device/measurements sub-objects).
    We flatten it here so the transform stage receives simple flat dicts.

    Flattening:
      geo.lat, geo.lon, geo.alt, geo.country → top-level fields
      device.sensor_id → sensor_id
      measurements → kept as a dict (transform_record extracts P1/P2)
    """
    pending_dir.mkdir(parents=True, exist_ok=True)

    # ── CDC: read only records newer than watermark ────────────────────────────
    if watermark_ts:
        rows = session.execute(
            """SELECT location_id, ingest_ts, event_ts, event_id, payload
               FROM tenantB_bronze.records
               WHERE ingest_ts > %s LIMIT %s ALLOW FILTERING""",
            (watermark_ts, max_records),
        )
    else:
        rows = session.execute(
            f"SELECT location_id, ingest_ts, event_ts, event_id, payload "
            f"FROM tenantB_bronze.records LIMIT {max_records}"
        )

    records = list(rows)
    if not records:
        return [], 0, "local"

    cache_file  = pending_dir / f"tenantB_{run_id}.jsonl"
    total_bytes = 0

    with open(cache_file, "w") as fh:
        for row in records:
            try:
                payload_obj = json.loads(row.payload)
            except Exception:
                payload_obj = {}

            # Extract from nested sub-objects and flatten
            geo = payload_obj.get("geo")    or {}
            dev = payload_obj.get("device") or {}
            rec = {
                "location_id":  row.location_id,
                "ingest_ts":    str(row.ingest_ts),
                "event_ts":     row.event_ts,
                "event_id":     row.event_id,
                # Flatten geo sub-object
                "lat":          geo.get("lat"),
                "lon":          geo.get("lon"),
                "alt":          geo.get("alt"),
                "country":      geo.get("country"),
                # Flatten device sub-object
                "sensor_id":    dev.get("sensor_id"),
                # Keep measurements dict for has_pm_data derivation in transform
                "measurements": payload_obj.get("measurements") or {},
            }
            line = json.dumps(rec) + "\n"
            fh.write(line)
            total_bytes += len(line.encode("utf-8"))

    cache_mode = "local"

    # ── Optional GCS upload (non-fatal) ───────────────────────────────────────
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

    return [cache_file], total_bytes, cache_mode


# ── STAGE 2: Transform + Load .jsonl → silver Cassandra ───────────────────────

def transform_and_load(
    session,
    cache_files: List[Path],
    processed_dir: Path,
) -> Dict[str, Any]:
    """
    Read .jsonl cache, transform each record, insert to silver, move cache file.
    Same structure as tenantA but targets tenantB_silver.geo_measurements.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    prepared = session.prepare(
        """INSERT INTO tenantB_silver.geo_measurements
           (location_id, silver_ts, event_ts, event_id, ingest_ts,
            lat, lon, alt, country, sensor_id, pm10, pm2_5, has_pm_data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    total_records = 0
    total_errors  = 0
    max_ingest_ts = None
    t_start       = time.time()

    for cache_file in cache_files:
        with open(cache_file) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw    = json.loads(line)
                    silver = transform_record(raw)
                    session.execute(prepared, (
                        silver["location_id"], silver["silver_ts"],
                        silver["event_ts"],    int(silver["event_id"] or 0),
                        silver["ingest_ts"],   silver["lat"],
                        silver["lon"],         silver["alt"],
                        silver["country"],
                        str(silver["sensor_id"]) if silver["sensor_id"] else None,
                        silver["pm10"],        silver["pm2_5"],
                        silver["has_pm_data"],
                    ))
                    total_records += 1
                    its = raw.get("ingest_ts")
                    if its and (max_ingest_ts is None or its > max_ingest_ts):
                        max_ingest_ts = its
                except Exception as e:
                    total_errors += 1
                    log_event("transform_load", "record_error",
                              {"error": str(e), "line": line[:120]})

        # Move local file: pending → processed
        dest = processed_dir / cache_file.name
        cache_file.rename(dest)

        # Move in GCS: upload to processed/, delete from pending/
        if USE_GCS:
            try:
                gcs_upload_file(dest, "processed")
                gcs_delete_blob(f"{GCS_PREFIX}/pending/{cache_file.name}")
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
    parser = argparse.ArgumentParser(description="TenantB Silver Pipeline")
    parser.add_argument("--run-id",        default=str(uuid.uuid4())[:8])
    parser.add_argument("--max-records",   type=int,
                        default=int(os.getenv("MAX_RECORDS", "3000")))  # stricter limit for tenantB
    parser.add_argument("--cassandra",     default=os.getenv("CASSANDRA_HOST", "cassandra"))
    parser.add_argument("--pending-dir",
                        default=os.getenv("CACHE_PENDING_DIR",   "/app/cache/tenantB/pending"))
    parser.add_argument("--processed-dir",
                        default=os.getenv("CACHE_PROCESSED_DIR", "/app/cache/tenantB/processed"))
    args = parser.parse_args()

    run_id        = args.run_id
    pending_dir   = Path(args.pending_dir)
    processed_dir = Path(args.processed_dir)
    t_total       = time.time()

    log_event("start", "ok", {
        "run_id":      run_id,
        "max_records": args.max_records,
        "gcs_enabled": USE_GCS,
        "gcs_bucket":  GCS_BUCKET or None,
    })

    # ── Cassandra connect with retry ──────────────────────────────────────────
    # 10 attempts, backoff: 3s, 6s, 9s, ..., 27s
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
            time.sleep(3 * _attempt)

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

    if not cache_files:
        log_event("extract", "no_new_data", {"watermark": watermark})
        cluster.shutdown()
        sys.exit(0)

    # ── STAGE 2: Transform + Load ─────────────────────────────────────────────
    try:
        log_event("transform_load", "start", {"files": len(cache_files)})
        result = transform_and_load(session, cache_files, processed_dir)
        log_event("transform_load", "ok", result)
        if result["max_ingest_ts"]:
            set_watermark(session, result["max_ingest_ts"])
            log_event("watermark", "updated", {"new_watermark": result["max_ingest_ts"]})
    except Exception as e:
        log_event("transform_load", "error", {"error": str(e)})
        cluster.shutdown()
        sys.exit(1)

    # ── COMPLETE line — all 8 metric fields required by batchmanager ──────────
    elapsed_sec = round(time.time() - t_total, 3)
    log_event("complete", "ok", {
        "run_id":          run_id,
        "elapsed_sec":     elapsed_sec,
        "extract_sec":     extract_sec,
        "transform_sec":   result["transform_sec"],
        "data_size_bytes": data_size_bytes,
        "cache_mode":      cache_mode,
        "records_loaded":  result["records_loaded"],
        "errors":          result["errors"],
    })

    cluster.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    main()
