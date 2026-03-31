#!/usr/bin/env python3
"""
streamanalyticsapp.py  —  mysimbdp Stream Analytics App (TenantA)
==================================================================

SCENARIO
--------
Real-Time Air Quality Alert System.
TenantA streams sensor readings (PM2.5, PM10, temperature) from
sensor.community into Kafka. This PySpark Structured Streaming job
consumes those records, computes sliding-window aggregations per country,
detects threshold breaches, and:
  1. Writes analytics results to Cassandra (tenanta_analytics.window_results)
  2. Publishes alerts back to Kafka topic (tenantA.alerts) in near real-time

DESIGN DECISIONS
----------------
- Keyed stream by 'country' for windowed group-by (matches assignment keyed stream)
- Event time from 'timestamp' field in the producer message
- Watermark of 2 minutes to handle late/out-of-order API records
- Sliding window: 5 minutes duration, sliding every 1 minute
- Alert condition: avg PM2.5 > PM25_ALERT_THRESHOLD or avg PM10 > PM10_ALERT_THRESHOLD
- foreachBatch used so we can write to both Cassandra AND Kafka in one micro-batch

INPUT SCHEMA (matches tenantA_producer.py flat JSON exactly)
-------------------------------------------------------------
  sensor_id, timestamp, country, pm2_5_P2, pm10_P1, lat, lon, alt, sensor_type

OUTPUT — Cassandra: tenanta_analytics.window_results
-----------------------------------------------------
  country, window_start, window_end, avg_pm25, avg_pm10,
  max_pm25, record_count, alert_fired

OUTPUT — Kafka: tenantA.alerts (only when threshold breached)
-------------------------------------------------------------
  { "country": "FI", "window_start": "...", "avg_pm25": 45.2,
    "alert_level": "WARNING", "ts": "..." }

RUNNING
-------
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    --conf spark.cassandra.connection.host=cassandra \
    streaming/streamanalyticsapp.py
"""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    avg, max as spark_max, count, lit, when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, BooleanType
)

# ── configuration (override via env vars in docker-compose) ───────────────────

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP",     "broker:29092")
KAFKA_INPUT_TOPIC   = os.getenv("KAFKA_INPUT_TOPIC",   "tenantA.bronze.raw")
KAFKA_ALERT_TOPIC   = os.getenv("KAFKA_ALERT_TOPIC",   "tenantA.alerts")
CASSANDRA_HOST      = os.getenv("CASSANDRA_HOST",       "cassandra")
CASSANDRA_KEYSPACE  = os.getenv("CASSANDRA_KEYSPACE",  "tenanta_analytics")
CASSANDRA_TABLE     = os.getenv("CASSANDRA_TABLE",      "window_results")

# Window parameters (override for Part 2 Q3 parallelism/window tests)
WINDOW_DURATION     = os.getenv("WINDOW_DURATION",     "5 minutes")
WINDOW_SLIDE        = os.getenv("WINDOW_SLIDE",        "1 minute")
WATERMARK_DELAY     = os.getenv("WATERMARK_DELAY",     "2 minutes")
TRIGGER_INTERVAL    = os.getenv("TRIGGER_INTERVAL",    "30 seconds")

# Alert thresholds (μg/m³ — WHO 24h guidelines: PM2.5=15, PM10=45)
PM25_ALERT_THRESHOLD = float(os.getenv("PM25_ALERT_THRESHOLD", "15.0"))
PM10_ALERT_THRESHOLD = float(os.getenv("PM10_ALERT_THRESHOLD", "45.0"))

# Spark parallelism (Part 2 Q5)
SPARK_PARTITIONS     = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))


# ── Input schema — must match tenantA_producer.py exactly ────────────────────
# Any field not in this schema is silently dropped.
# If a required field is missing in the message, it becomes null (handled below).

INPUT_SCHEMA = StructType([
    StructField("tenant_id",    StringType(),  True),
    StructField("event_id",     LongType(),    True),
    StructField("timestamp",    StringType(),  True),   # event time — "2026-03-10 23:00:00"
    StructField("sensor_id",    LongType(),    True),
    StructField("lat",          DoubleType(),  True),
    StructField("lon",          DoubleType(),  True),
    StructField("alt",          DoubleType(),  True),
    StructField("country",      StringType(),  True),
    StructField("sensor_type",  StringType(),  True),
    StructField("pm10_P1",      DoubleType(),  True),   # PM10 μg/m³
    StructField("pm2_5_P2",     DoubleType(),  True),   # PM2.5 μg/m³
    StructField("ingest_ts_utc",StringType(),  True),
])


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_batch(batch_df: DataFrame, batch_id: int):
    """
    foreachBatch sink: called once per micro-batch with the aggregated results.

    Writes to TWO sinks:
      1. Cassandra — all window results (always)
      2. Kafka     — alert messages only (when threshold breached)

    Using foreachBatch instead of separate writeStream calls ensures both
    writes happen atomically within the same micro-batch, preventing the
    case where Cassandra gets the result but Kafka does not (or vice versa).
    """
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty — skipping")
        return

    # Cache the batch so we don't recompute it for each sink
    batch_df.cache()
    count_rows = batch_df.count()
    print(f"[batch {batch_id}] {count_rows} window results to write  ts={now_iso()}")

    # ── Sink 1: Write ALL results to Cassandra ────────────────────────────────
    try:
        (batch_df
            .write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
            .save())
        print(f"[batch {batch_id}] ✅ Cassandra write ok ({count_rows} rows)")
    except Exception as e:
        print(f"[batch {batch_id}] ❌ Cassandra write failed: {e}")

    # ── Sink 2: Publish ALERTS to Kafka (near real-time back to tenant) ───────
    # Only rows where alert_fired = True are sent to the alert topic.
    # This satisfies the assignment requirement: results sent back to tenant
    # "under certain conditions" (threshold breach).
    alerts_df = batch_df.filter(col("alert_fired") == True)

    if not alerts_df.rdd.isEmpty():
        alert_count = alerts_df.count()

        # Build Kafka value JSON from the alert columns
        import pyspark.sql.functions as F

        kafka_alerts = alerts_df.select(
            F.to_json(F.struct(
                col("country"),
                col("window_start").cast("string").alias("window_start"),
                col("window_end").cast("string").alias("window_end"),
                col("avg_pm25"),
                col("avg_pm10"),
                col("max_pm25"),
                col("record_count"),
                F.lit(now_iso()).alias("alert_ts"),
                F.when(col("avg_pm25") > PM25_ALERT_THRESHOLD, "PM2.5_BREACH")
                 .otherwise("PM10_BREACH").alias("alert_reason"),
            )).alias("value"),
            col("country").alias("key")    # partition alerts by country
        )

        try:
            (kafka_alerts
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", KAFKA_ALERT_TOPIC)
                .save())
            print(f"[batch {batch_id}] 🚨 {alert_count} alerts sent to {KAFKA_ALERT_TOPIC}")
        except Exception as e:
            print(f"[batch {batch_id}] ❌ Kafka alert write failed: {e}")

    batch_df.unpersist()


def main():
    print(f"==> streamanalyticsapp starting  ts={now_iso()}")
    print(f"    Kafka input : {KAFKA_BOOTSTRAP} / {KAFKA_INPUT_TOPIC}")
    print(f"    Kafka alerts: {KAFKA_ALERT_TOPIC}")
    print(f"    Cassandra   : {CASSANDRA_HOST} / {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}")
    print(f"    Window      : duration={WINDOW_DURATION}  slide={WINDOW_SLIDE}")
    print(f"    Watermark   : {WATERMARK_DELAY}")
    print(f"    Thresholds  : PM2.5>{PM25_ALERT_THRESHOLD}  PM10>{PM10_ALERT_THRESHOLD}")
    print(f"    Partitions  : {SPARK_PARTITIONS}")

    # ── Build SparkSession ────────────────────────────────────────────────────
    spark = (SparkSession.builder
        .appName("mysimbdp-streamanalyticsapp-tenantA")
        # Cassandra connector config
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        # Parallelism — tunable for Part 2 Q5
        .config("spark.sql.shuffle.partitions", str(SPARK_PARTITIONS))
        .config("spark.default.parallelism",    str(SPARK_PARTITIONS))
        # Streaming micro-batch state store
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")   # reduce noise in logs

    # ── Step 1: Read raw bytes from Kafka ────────────────────────────────────
    raw_kafka = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .option("startingOffsets", "latest")          # only new messages
        .option("failOnDataLoss", "false")            # tolerate Kafka offset gaps
        .option("kafka.group.id", "streamanalyticsapp-tenantA")
        .load()
    )

    # ── Step 2: Deserialize JSON and enforce schema ───────────────────────────
    # 'value' from Kafka is bytes → cast to string → parse with our schema.
    # Records that don't match the schema get null values (handled in step 3).
    # Records with completely unparseable JSON are dropped silently by from_json
    # (Spark behavior) — erroneous records test uses explicit null checks below.
    parsed = (raw_kafka
        .select(
            from_json(col("value").cast("string"), INPUT_SCHEMA).alias("d"),
            col("timestamp").alias("kafka_ts")   # Kafka metadata timestamp (processing time)
        )
        .select("d.*", "kafka_ts")
    )

    # ── Step 3: Filter erroneous records ─────────────────────────────────────
    # Records with null country or null event time are silently dropped here.
    # A separate bad-records branch (for Part 3 Q2) would tee these off instead.
    # For Part 2 Q4: we log drop counts in the foreachBatch function.
    valid = (parsed
        .filter(col("country").isNotNull())
        .filter(col("timestamp").isNotNull())
        # Convert event timestamp string → TimestampType for windowing
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .filter(col("event_time").isNotNull())
    )

    # ── Step 4: Apply watermark and sliding window aggregation ────────────────
    # Watermark: tell Spark how late data can arrive — records older than
    # (max_event_time - WATERMARK_DELAY) are dropped.  This controls state size.
    #
    # Sliding window: 5-min window moving every 1 min.
    # Each record contributes to 5 overlapping windows.
    # Group by (window, country) → one row per country per window.
    aggregated = (valid
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE),
            col("country")
        )
        .agg(
            avg("pm2_5_P2").alias("avg_pm25"),
            avg("pm10_P1").alias("avg_pm10"),
            spark_max("pm2_5_P2").alias("max_pm25"),
            spark_max("pm10_P1").alias("max_pm10"),
            count("*").alias("record_count"),
        )
        # Flatten the window struct into two timestamp columns
        .select(
            col("country"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_pm25"),
            col("avg_pm10"),
            col("max_pm25"),
            col("max_pm10"),
            col("record_count"),
            # Alert flag: True when either PM2.5 or PM10 breaches threshold
            when(
                (col("avg_pm25") > PM25_ALERT_THRESHOLD) |
                (col("avg_pm10") > PM10_ALERT_THRESHOLD),
                True
            ).otherwise(False).alias("alert_fired"),
        )
    )

    # ── Step 5: Write via foreachBatch (Cassandra + Kafka alerts) ─────────────
    # 'update' output mode: emit updated windows as soon as they have new data.
    # Watermark ensures late records don't reopen old windows indefinitely.
    query = (aggregated
        .writeStream
        .outputMode("update")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoint/streamanalyticsapp")
        .start()
    )

    print(f"==> Streaming query started. Waiting for data on {KAFKA_INPUT_TOPIC} ...")
    query.awaitTermination()


if __name__ == "__main__":
    main()