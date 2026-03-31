#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh  —  mysimbdp Assignment 3 (extends Assignment 2 bootstrap)
# =============================================================================
#
# This script:
#   1. Starts all Docker Compose services (Kafka, Cassandra, Spark, Producer, etc.)
#   2. Waits for Kafka and Cassandra to be ready
#   3. Creates ALL Kafka topics (Assignment 2 + new Assignment 3 alert topic)
#   4. Creates ALL Cassandra keyspaces/tables (Assignment 2 + new analytics table)
#
# USAGE
#   chmod +x bootstrap.sh
#   ./bootstrap.sh
#
# Re-running is safe — topic creation and CQL statements are idempotent.
# =============================================================================

set -euo pipefail

BROKER_CONTAINER="broker"
CASSANDRA_SERVICE="cassandra"
CASSANDRA_CONTAINER="cassandra"

BOOTSTRAP="localhost:9092"

# ── Kafka topics ──────────────────────────────────────────────────────────────
TOPIC_A="tenantA.bronze.raw"        # Assignment 2 (reused)
TOPIC_B="tenantB.bronze.raw"        # Assignment 2 (reused)
TOPIC_ALERTS="tenantA.alerts"       # Assignment 3 NEW — analytics alert output
PARTITIONS=6
REPL=1

echo "==> Starting all services (docker compose up -d)..."
docker compose up -d

# ── Detect compose network ────────────────────────────────────────────────────
echo "==> Detecting docker compose network..."
COMPOSE_NETWORK=""
for i in {1..30}; do
  CID="$(docker compose ps -q 2>/dev/null | head -n 1 || true)"
  if [[ -n "${CID}" ]]; then
    COMPOSE_NETWORK="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "${CID}" | head -n 1 || true)"
  fi
  if [[ -n "${COMPOSE_NETWORK}" ]]; then break; fi
  echo "  ...waiting for network (${i}/30)"
  sleep 1
done

if [[ -z "${COMPOSE_NETWORK}" ]]; then
  echo "❌ Could not detect docker compose network."
  docker compose ps || true
  exit 1
fi
echo "✅ Network: ${COMPOSE_NETWORK}"

# ── CQL helper (runs cqlsh in a temp container) ───────────────────────────────
cql() {
  local stmt="$1"
  docker run --rm --network "${COMPOSE_NETWORK}" cassandra:4.1 \
    cqlsh "${CASSANDRA_SERVICE}" 9042 -e "$stmt"
}

# ── Wait for Kafka ────────────────────────────────────────────────────────────
echo "==> Waiting for Kafka..."
for i in {1..60}; do
  if docker exec -i "${BROKER_CONTAINER}" bash -lc \
    "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${BOOTSTRAP} >/dev/null 2>&1"; then
    echo "✅ Kafka ready"
    break
  fi
  echo "  ...waiting (${i}/60)"
  sleep 2
  if [[ "$i" == "60" ]]; then echo "❌ Kafka timeout"; exit 1; fi
done

# ── Create Kafka topics (idempotent) ─────────────────────────────────────────
create_topic() {
  local topic="$1"
  if docker exec -i "${BROKER_CONTAINER}" bash -lc \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --list | grep -x '${topic}' >/dev/null 2>&1"; then
    echo "==> Topic exists: ${topic}"
  else
    echo "==> Creating topic: ${topic}"
    docker exec -i "${BROKER_CONTAINER}" bash -lc \
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} \
       --create --topic '${topic}' --partitions ${PARTITIONS} --replication-factor ${REPL}"
    echo "✅ Created: ${topic}"
  fi
}

create_topic "${TOPIC_A}"
create_topic "${TOPIC_B}"
create_topic "${TOPIC_ALERTS}"    # NEW for Assignment 3

# ── Wait for Cassandra ────────────────────────────────────────────────────────
echo "==> Waiting for Cassandra port..."
for i in {1..300}; do
  if docker run --rm --network "${COMPOSE_NETWORK}" bash:5.2 \
      bash -lc "cat < /dev/null > /dev/tcp/${CASSANDRA_SERVICE}/9042" >/dev/null 2>&1; then
    echo "✅ Cassandra port open"
    break
  fi
  echo "  ...waiting (${i}/300)"
  sleep 2
  if [[ "$i" == "300" ]]; then echo "❌ Cassandra port timeout"; exit 1; fi
done

echo "==> Waiting for Cassandra CQL..."
for i in {1..300}; do
  if cql "SELECT now() FROM system.local;" >/dev/null 2>&1; then
    echo "✅ Cassandra CQL ready"
    break
  fi
  echo "  ...waiting (${i}/300)"
  sleep 2
  if [[ "$i" == "300" ]]; then echo "❌ Cassandra CQL timeout"; exit 1; fi
done

# =============================================================================
# CASSANDRA SCHEMA — Assignment 2 (unchanged, idempotent)
# =============================================================================
echo "==> Creating Assignment 2 schema (idempotent)..."

cql "CREATE KEYSPACE IF NOT EXISTS tenanta_bronze WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS tenanta_bronze.records (
        sensor_id text,
        ingest_ts timestamp,
        event_ts text,
        event_id bigint,
        topic text,
        payload text,
        PRIMARY KEY ((sensor_id), ingest_ts)
      ) WITH CLUSTERING ORDER BY (ingest_ts DESC);"

cql "CREATE KEYSPACE IF NOT EXISTS tenantb_bronze WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS tenantb_bronze.records (
        location_id text,
        ingest_ts timestamp,
        event_ts text,
        event_id bigint,
        topic text,
        payload text,
        PRIMARY KEY ((location_id), ingest_ts)
      ) WITH CLUSTERING ORDER BY (ingest_ts DESC);"

cql "CREATE KEYSPACE IF NOT EXISTS tenanta_silver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS tenanta_silver.air_quality (
        sensor_id   text,
        silver_ts   text,
        event_ts    text,
        event_id    bigint,
        ingest_ts   text,
        lat         double,
        lon         double,
        alt         double,
        country     text,
        sensor_type text,
        pm10        double,
        pm2_5       double,
        aqi_bucket  text,
        PRIMARY KEY ((sensor_id), silver_ts)
    ) WITH CLUSTERING ORDER BY (silver_ts DESC);"

cql "CREATE KEYSPACE IF NOT EXISTS tenantb_silver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS tenantb_silver.geo_measurements (
        location_id text,
        silver_ts   text,
        event_ts    text,
        event_id    bigint,
        ingest_ts   text,
        lat         double,
        lon         double,
        alt         double,
        country     text,
        sensor_id   text,
        pm10        double,
        pm2_5       double,
        has_pm_data boolean,
        PRIMARY KEY ((location_id), silver_ts)
    ) WITH CLUSTERING ORDER BY (silver_ts DESC);"

cql "CREATE KEYSPACE IF NOT EXISTS platform_logs WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS platform_logs.streaming_metrics (
        tenant_id text,
        worker_id text,
        ts timestamp,
        window_sec int,
        avg_ingest_ms double,
        records bigint,
        bytes bigint,
        errors bigint,
        PRIMARY KEY ((tenant_id), ts)
      ) WITH CLUSTERING ORDER BY (ts DESC);"
cql "CREATE TABLE IF NOT EXISTS platform_logs.silver_pipeline_logs (
        tenant_id        text,
        run_id           text,
        started_at       text,
        finished_at      text,
        status           text,
        records_loaded   bigint,
        errors           bigint,
        elapsed_sec      double,
        extract_sec      double,
        transform_sec    double,
        data_size_bytes  bigint,
        cache_mode       text,
        pipeline_script  text,
        detail           text,
        PRIMARY KEY ((tenant_id), started_at)
    ) WITH CLUSTERING ORDER BY (started_at DESC);"
cql "CREATE TABLE IF NOT EXISTS platform_logs.silver_watermarks (
        tenant_id          text PRIMARY KEY,
        last_processed_ts  text
    );"

# =============================================================================
# CASSANDRA SCHEMA — Assignment 3 NEW
# =============================================================================
echo "==> Creating Assignment 3 analytics schema (NEW)..."

# Analytics results keyspace — stores windowed aggregation results from Spark
cql "CREATE KEYSPACE IF NOT EXISTS tenanta_analytics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Window results table
# Partition key: country  — all windows for the same country co-located
# Clustering key: window_start DESC — most recent windows first
cql "CREATE TABLE IF NOT EXISTS tenanta_analytics.window_results (
        country       text,
        window_start  timestamp,
        window_end    timestamp,
        avg_pm25      double,
        avg_pm10      double,
        max_pm25      double,
        max_pm10      double,
        record_count  bigint,
        alert_fired   boolean,
        PRIMARY KEY ((country), window_start)
      ) WITH CLUSTERING ORDER BY (window_start DESC);"

echo ""
echo "==> Verifying keyspaces..."
cql "DESCRIBE KEYSPACES;"

echo ""
echo "✅ Bootstrap complete."
echo ""
echo "NEXT STEPS:"
echo "  1. All services are running. Check: docker compose ps"
echo "  2. Producer is streaming to tenantA.bronze.raw automatically."
echo "  3. PySpark job is consuming and writing to tenanta_analytics.window_results."
echo "  4. Check analytics results:"
echo "     docker exec -it cqlsh cqlsh cassandra 9042"
echo "     SELECT * FROM tenanta_analytics.window_results LIMIT 20;"
echo "  5. Monitor alerts:"
echo "     docker logs -f tenant-alert-consumer"
echo ""
echo "SPEED TESTS (from host machine):"
echo "  python streaming/speed_test.py --speed slow   --duration 60"
echo "  python streaming/speed_test.py --speed medium --duration 60"
echo "  python streaming/speed_test.py --speed fast   --duration 60"
echo ""
echo "INJECT ERRORS (Part 2 Q4):"
echo "  python streaming/inject_errors.py --errors-per-type 10"