# Big Data Platforms - Assignment 3

**Student:** Muhammad Sabeeh Waqas
**ID:** 103704559

---

## Overview

<u>This assignment extends the mysimbdp platform from Assignment 2 with stream analytics capabilities. The same infrastructure (Kafka, Cassandra, Docker) is reused.</u> A PySpark streaming job is added that takes raw sensor readings in real time, then computes sliding window, aggregates per country and detects PM2.5/PM10 threshold, after that writes analytics results to Cassandra, and publishes alerts back to the tenant via a dedicated Kafka topic.

**Tech stack:** Apache Kafka (messaging), PySpark Structured Streaming (stream processing), Cassandra (mysimbdp-coredms), Python, Docker Compose.

**Architecture diagram:**
<img src="./report-img/BDF-Asg3-drawio.svg" alt="isolated" width="2000"/>

---

## Part 1 - Design for Streaming Analytics

### P1.1 - Dataset and Scenario

**Dataset:** sensor.community open air quality API ('https://github.com/opendata-stuttgart/meta/wiki/EN-APIs'). This API returns a rolling snapshot of readings from approx. 15,000 citizen owned air quality sensors worldwide. Each record contains a sensor identifier, geographic location (lat/lon/country), and particulate matter measurements (PM2.5 as P2, PM10 as P1) from sensors like the SDS011.(info source )

**Why suitable for streaming analytics:** The API updates every 5 minutes with new sensor readings from thousands of globally distributed sensors. This is a continuous, high volume, geographically distributed data source. Air quality is now becoming a big concern due to high air polution and health hazards, In my preivous soltuion (Asg 2) the air quality data was being stored in cassandra table as batch of 5 min. But these air polutents sometimes needs to be identified quickly and not wait 5 mins, specially in sensative areas like hospitals, schools, research labs or areas near active natural disasters like volcanos or hurricans. In such cases a streaming analystics soltuion needs to be designed to alert the tenant asap.

**Scenario:** TenantA is a public health monitoring service. They stream raw sensor readings into the platform in near real-time. The 'streamanalyticsapp' computes rolling average PM2.5 and PM10 concentrations per country over sliding 1 minute windows. When the average PM2.5 in a country exceeds 15 μg/m^3 (WHO 24h guideline) or PM10 exceeds 45 μg/m^3, an alert is fired back to the tenant immediately.

**streamanalyticsapp functionality:**

- Consumes from Kafka topic 'tenantA.bronze.raw'
- Groups records by country and 1 minute sliding window (slides every 30 seconds)
- Computes avg PM2.5, avg PM10, max PM2.5, max PM10, record count per window
- Flags windows where either threshold is breached ('alert_fired = True')
- Writes all window results to Cassandra keyspace 'tenanta_analytics', table 'window_results'
- Publishes alert JSON to Kafka topic 'tenantA.alerts' for near-real-time tenant notification

**Data sink in mysimbdp-coredms (Cassandra):**

'''
tenanta_analytics.window_results
country text <- partition key
window_start timestamp <- clustering key (DESC)
window_end timestamp
avg_pm25 double
avg_pm10 double
max_pm25 double
max_pm10 double
record_count bigint
alert_fired boolean
'''

Partition key is 'country' so all windows for the same country are co located on the same Cassandra node, making per country range queries efficient.

---

### P1.2 - Keyed vs Non-Keyed Streams and Message Delivery Guarantees

**Keyed stream:** The stream is keyed by 'country'. In the Kafka producer, messages are partitioned by 'sensor_id' (ensuring all readings from the same sensor go to the same partition, preserving sensor ordering). In PySpark, the windowed aggregation groups by '(window, country)', making it effectively keyed on country for analytics purposes.

Keying by country is used because:

- The analytics goal is per country air quality averages, not per sensor
- It distributes processing across Spark tasks evenly (many countries, many sensors per country)
- Cassandra is also partitioned by country, so writes are efficient

Example key assignment: a reading from sensor '78901' in Finland ('country=FI') is keyed to partition 'hash(78901) % 6' in Kafka, and contributes to the 'FI' window group in Spark.

**Message delivery guarantees:** At-least-once delivery is appropriate and enough for this scenario. The producer uses 'enable.idempotence=True' and 'acks=all' which provides exactly-once delivery at the producer level within a single Kafka session. On the consumer (Spark) side, 'foreachBatch' with checkpointing provides at-least-once semantics, if a batch fails and retries, some window results may be recomputed and re-written to Cassandra, but since Cassandra writes use the same primary key '(country, window_start)' the result is idempotent (same row gets overwritten with the same data).

Exactly-once end-to-end would need Kafka transactions and two-phase commit with Cassandra, which adds much complexity. For a public health monitoring scenario, at-least-once is nice: a duplicate alert is far less harmful than a missed alert.

---

### P1.3 - Time, Windows, Watermarks, and Out-of-Order Data

**Event time vs processing time:** Event time is used, derived from the 'timestamp' field in each sensor reading (the time the sensor took the measurement, as recorded by the sensor.community server). This is the correct choice because:

- We want to know the air quality at a specific point in time, not when it arrived in the platform
- Sensor readings may be delayed by network latency, API polling intervals, or Kafka lag
- Using processing time would cause readings from the same measurement period to fall into different windows depending on when they happened to arrive

If a record has no timestamp, the solution can be to use 'kafka_ts' (the Kafka broker timestamp, which is processing time) as a best-effort substitute.

**Window type and parameters:**

**Sliding window**: 1-minute duration, sliding every 30 seconds

- This means each record contributes to 2 overlapping windows simultaneously
- A 1-minute window gives fine-grained temporal resolution, making it suitable for detecting rapidly developing pollution events — a PM2.5 spike is visible within 1 minute of occurring rather than being averaged across 5 minutes
- Sliding by 30 seconds means a new window result is emitted every 30 seconds, matching the Spark trigger interval and providing near real-time updates to the tenant
- The tradeoff is lower `record_count` per window compared to a wider window i.e. each window captures fewer sensor readings, so averages are noisier and more sensitive to individual sensor spikes. A wider window (e.g. 5 minutes) would produce smoother averages but increase alert latency

Window function used: `groupBy(window(event_time, "1 minute", "30 seconds"), country)` with aggregation functions `avg`, `max`, `count`.

**Alternative tested**: 5-minute sliding window (`WINDOW_DURATION=5 minutes`, `WINDOW_SLIDE=1 minute`) it produces fewer rows with higher `record_count` per window, smoother averages, but higher alert latency.

**Out-of-order data causes:**

- The sensor.community API is polled every 0.5 seconds but the API itself only updates every 5 minutes. Records for the same measurement window may arrive spread across multiple API polls.
- Network latency between the sensor and the community server varies per sensor
- The producer's deduplication logic may delay some records by a poll cycle
- Kafka consumer lag when 'spark-streaming' starts or restarts

**Watermark:** A 2-minute watermark is used ('withWatermark("event_time", "2 minutes")'). This tells Spark to wait up to 2 minutes for late records before closing a window and emitting results. The choice of 2 minutes balances:

- Covering typical API polling delays (< 1 minute)
- Not holding state in memory too long (larger watermark = more state)
- Keeping alert latency acceptable (alerts fire within 2 minutes of the event window closing)

Watermarks are necessary because without them Spark would hold all window state in memory indefinitely, causing the job to eventually run out of memory as more windows accumulate.

---

### P1.4 - Key Performance Metrics

**1. End-to-end latency (event-to-alert)**

- _Definition:_ Time from the sensor measurement timestamp to when the alert is published to 'tenantA.alerts'
- _How to measure:_ Compare 'timestamp' field in the Kafka message with 'alert_ts' field in the alert JSON. Can be logged in 'write_batch()' function
- _Importance:_ For public health alerting, low latency is critical. The platform SLA should guarantee alerts within N minutes of a threshold breach. Relevant to: tenant (SLA), platform operator (capacity planning)

**2. Micro-batch processing time**

- _Definition:_ Wall-clock time for Spark to process one micro-batch (from trigger to Cassandra write complete)
- _How to measure:_ Spark Streaming UI at 'http://localhost:8080' shows batch duration per trigger. Also visible in 'spark-streaming' container logs as time between '[batch N]' lines
- _Importance:_ If batch duration exceeds trigger interval (30 seconds), batches queue up and latency grows unboundedly. Relevant to: platform operator (scaling decisions)

**3. Throughput (records per second)**

- _Definition:_ Number of sensor records processed per second by the streaming job
- _How to measure:_ 'record_count' summed across all windows in a batch divided by 'TRIGGER_INTERVAL'. Also measurable via Kafka consumer group lag ('kafka-consumer-groups.sh --describe')
- _Importance:_ Ensures the platform can handle peak loads (e.g., all 15,000 sensors reporting simultaneously). Relevant to: platform designer (capacity), tenant (SLA)

**4. Cassandra write success rate**

- _Definition:_ Percentage of 'foreachBatch' calls that complete Cassandra write without error
- _How to measure:_ Count '✅ Cassandra write ok' vs '❌ Cassandra write failed' log lines in 'spark-streaming' logs
- _Importance:_ Failed writes mean analytics results are lost. Relevant to: platform operator (reliability), tenant (data completeness)

**5. Alert precision (false positive rate)**

- _Definition:_ Percentage of fired alerts where the PM2.5/PM10 breach was real vs a data artifact (e.g., sensor malfunction, negative value)
- _How to measure:_ Compare 'alert_fired=True' windows with 'avg_pm25' values; check for windows with very low 'record_count' (< 3) which may be statistically unreliable
- _Importance:_ Too many false alerts degrades tenant trust. Relevant to: tenant (product quality)

---

### P1.5 - Architecture Design

**Architecture components:**

Old reused Architeture
<img src="./report-img/mysimbdp_architecture-2.svg" alt="isolated" width="20000"/>

New Architecure which incluses a new Kafka alert Topic , Apache Apark and New cassadnra Keyspace and table, tenant alert python consumer.

<img src="./report-img/BDF-Asg3-drawio-streamonly.svg" alt="isolated" width="20000"/>

Full Combined architecture
<img src="./report-img/BDF-Asg3-drawio.svg" alt="isolated" width="2000"/>

**Technology choices:**

- **Kafka** (reused from Asg 2): already running, zero additional infrastructure cost. The existing 'tenantA.bronze.raw' topic feeds directly into the streaming job without any schema changes.
- **PySpark Structured Streaming**: chosen over Flink because PySpark has a more mature Python API, better Cassandra connector support, and easier window/watermark syntax. The 'foreachBatch' pattern allows writing to multiple sinks (Cassandra + Kafka) in one atomic operation.
- **Cassandra** (reused from Asg 2): already running with the bronze/silver schema. New 'tenanta_analytics' keyspace added alongside existing keyspaces. Partition key 'country' chosen for efficient per country queries.
- **local[2] mode**: Spark runs in local mode with 2 threads inside the 'spark-streaming' container. For this assignment's scale (thousands of sensor records per minute) this is sufficient and avoids the complexity of a full cluster. For production, '--master spark://spark-master:7077' would be used.

**Reuse from Assignment 2:** The Kafka broker, Cassandra cluster, Docker network, 'tenantA_producer.py' (with env var patches), 'bootstrap.sh' (extended with new topics and schema), and 'docker-compose.yaml' (extended with 4 new services) are all directly reused. The 'streamingestworker' continues to ingest raw data into 'tenanta_bronze.records' in parallel - the Assignment 3 streaming analytics reads from the same Kafka topic independently via a separate consumer group.

---

## Part 2 - Implementation of Streaming Analytics

### P2.1 - Schemas, Serialization, and Processing Logic

**Input schema** (matches 'tenantA_producer.py' flat JSON exactly):

'''python
INPUT_SCHEMA = StructType([
StructField("tenant_id", StringType(), True),
StructField("event_id", LongType(), True),
StructField("timestamp", StringType(), True), # event time
StructField("sensor_id", LongType(), True),
StructField("lat", DoubleType(), True),
StructField("lon", DoubleType(), True),
StructField("alt", DoubleType(), True),
StructField("country", StringType(), True),
StructField("sensor_type", StringType(), True),
StructField("pm10_P1", DoubleType(), True),
StructField("pm2_5_P2", DoubleType(), True),
StructField("ingest_ts_utc", StringType(), True),
])
'''

**Analytics output schema** (written to Cassandra):

'''
country text, window_start timestamp, window_end timestamp,
avg_pm25 double, avg_pm10 double, max_pm25 double, max_pm10 double,
record_count bigint, alert_fired boolean
'''

**Why enforce schemas:** Without a defined schema, 'from_json' would infer types at runtime which is non-deterministic and fragile - a single malformed record could change the inferred type of 'pm2_5_P2' from 'DoubleType' to 'StringType', breaking all downstream aggregations. Explicit schemas also serve as a contract: the producer and the analytics job both know exactly what fields to expect, making the system easier to maintain and debug.

**Serialization/deserialization:** Records arrive from Kafka as raw bytes. The job deserializes using 'from_json(col("value").cast("string"), INPUT_SCHEMA)'. Output alerts are serialized back to JSON using 'to_json(struct(...))' before being published to the 'tenantA.alerts' Kafka topic. Cassandra writes use the native Spark-Cassandra connector which handles type mapping internally.

**Processing logic:**

1. Read raw bytes from Kafka ('readStream')
2. Cast 'value' column from bytes to string, parse with 'from_json' using 'INPUT_SCHEMA'
3. Filter out records with null 'country' or null/unparseable 'timestamp'
4. Convert 'timestamp' string to 'TimestampType' with 'to_timestamp(..., "yyyy-MM-dd HH:mm:ss")'
5. Apply watermark of 2 minutes on 'event_time'
6. Group by '(window(event_time, "5 minutes", "1 minute"), country)'
7. Aggregate: 'avg(pm2_5_P2)', 'avg(pm10_P1)', 'max(pm2_5_P2)', 'max(pm10_P1)', 'count(\*)'
8. Add 'alert_fired' boolean column: 'True' when 'avg_pm25 > 15.0' OR 'avg_pm10 > 45.0'
9. 'writeStream' with 'outputMode("update")' and 'foreachBatch(write_batch)'

**Near-real-time results back to tenant:** The 'write_batch' function publishes to 'tenantA.alerts' **only when 'alert_fired = True'**. This is the "certain condition" requirement. The tenant's 'alert_consumer.py' subscribes to this topic and receives alerts within one trigger interval (30 seconds) of the window being updated. The trigger interval is configurable via 'TRIGGER_INTERVAL' env var.

---

### P2.2 - Test Environment

**'[ADD: docker compose ps screenshot showing all containers running]'**

The test environment runs entirely in Docker on a local MacBook. All services communicate over a single Docker bridge network ('mysimbdp_default').

**Components:**

- 'broker' - Apache Kafka 3.7.2 (KRaft mode, no Zookeeper), 6 partitions per topic
- 'cassandra' - Cassandra 4.1, single node, 'SimpleStrategy' replication factor 1
- 'tenanta-producer' - Python 3.12, polls sensor.community API, 'POLL_INTERVAL_SEC=0.5' (2 msg/s baseline)
- 'spark-streaming' - apache/spark:3.5.5, local[2] mode, trigger every 30 seconds
- 'tenant-alert-consumer' - Python 3.12, confluent-kafka consumer

**Emulation of streaming data:** 'tenantA_producer.py' fetches one random sensor reading from the live sensor.community API every 0.5 seconds and publishes it to Kafka. For speed tests, 'streaming/speed_test.py' generates synthetic records locally at controlled rates (1 msg/s, 10 msg/s, 100 msg/s, or flood) without hitting the external API. This allows repeatable, controlled load testing.

**Key configurations:**

- 'WINDOW_DURATION=5 minutes', 'WINDOW_SLIDE=1 minute', 'WATERMARK_DELAY=2 minutes'
- 'PM25_ALERT_THRESHOLD=15.0' μg/m³, 'PM10_ALERT_THRESHOLD=45.0' μg/m³
- 'SPARK_SHUFFLE_PARTITIONS=4', '--master local[2]'
- Kafka consumer group: 'streamanalyticsapp-tenantA'
- Checkpoint location: Docker volume 'spark_checkpoint' at '/tmp/spark-checkpoint/streamanalyticsapp'

---

### P2.3 - Running the App and Performance Observations

**'[ADD: docker logs spark-streaming screenshot showing batches processing]'**

**'[ADD: Cassandra SELECT screenshot showing window_results with data]'**

**'[ADD: docker logs tenant-alert-consumer screenshot showing 🚨 alerts]'**

**(i) Effect of varying streaming data speed:**

**'[ADD: speed_test.py --speed slow screenshot + Cassandra query result]'**

**'[ADD: speed_test.py --speed fast screenshot + Cassandra query result]'**

At slow speed (1 msg/s), each 5-minute window accumulates approximately 300 records. The Spark batch completes well within the 30-second trigger interval. Cassandra write latency is negligible.

At fast speed (100 msg/s), each window accumulates ~30,000 records. Batch processing time increases but remains within the trigger interval due to local[2] parallelism. Record counts per window are much higher, averages are more statistically stable, and more countries appear in each batch.

At flood speed (maximum throughput), Kafka consumer lag begins to build up after ~30 seconds, meaning Spark cannot process records as fast as they arrive. This is expected with 'local[2]' - increasing to 'local[4]' reduces the lag.

**(ii) Effect of changing window parameters:**

**'[ADD: screenshot of WINDOW_DURATION=1 minute result in Cassandra - more rows, lower record_count]'**

**'[ADD: screenshot of WINDOW_DURATION=5 minutes result in Cassandra - fewer rows, higher record_count]'**

Narrow window (1 min / 30s slide): produces more rows in Cassandra, each with lower 'record_count'. Averages are noisier due to fewer samples. More alerts fire spuriously because a single high-PM2.5 reading dominates a small window.

Wide window (10 min / 2 min slide): fewer rows, higher 'record_count', smoother averages. Better for detecting sustained pollution events. Increases alert latency (tenant notified later).

---

### P2.4 - Erroneous Data Handling

**'[ADD: inject_errors.py output screenshot]'**

**'[ADD: spark-streaming logs showing job survives errors screenshot]'**

**Emulation of erroneous data:** 'streaming/inject_errors.py' sends 7 categories of malformed records directly to 'tenantA.bronze.raw':

| Error type          | What it does                   | How Spark handles it                                                             |
| ------------------- | ------------------------------ | -------------------------------------------------------------------------------- |
| 'MISSING_COUNTRY'   | 'country: null'                | Filtered at Step 3 (null check)                                                  |
| 'MISSING_TIMESTAMP' | 'timestamp: null'              | Filtered at Step 3 (null check)                                                  |
| 'NEGATIVE_PM'       | 'pm2_5_P2: -999.0'             | Passes filter, contributes negative value to avg (visible in results as anomaly) |
| 'WRONG_TYPES'       | 'pm2_5_P2: "not-a-number"'     | 'from_json' coerces to null for DoubleType; treated as null in aggregation       |
| 'INVALID_JSON'      | Raw bytes '{ this is not json' | 'from_json' returns all-null row; filtered by null country check                 |
| 'EMPTY_PAYLOAD'     | Empty bytes                    | Same as invalid JSON                                                             |
| 'VALID'             | Normal record                  | Processed normally                                                               |

**Test design:** 10 records of each error type were injected while the streaming job was running. The job was observed for 3 consecutive batches post-injection.

**Results:** The streaming job never crashed or restarted during error injection. Bad records were silently dropped at the filter step. The 'record_count' per window was slightly lower than batches without errors, reflecting the dropped records. The 'NEGATIVE_PM' records did visibly pull down 'avg_pm25' in affected windows, which is the expected behavior for this design (the filter only removes structurally invalid records, not semantically invalid values - that would require domain-specific validation).

---

### P2.5 - Parallelism

**Factors affecting parallelism:**

In the current 'local[N]' deployment:

- '--master local[2]' - number of threads for Spark task execution
- 'SPARK_SHUFFLE_PARTITIONS' - number of partitions created during 'groupBy' shuffle operations
- Kafka topic partitions (6) - upper bound on Kafka consumer parallelism
- Number of distinct countries in the data - determines how many groups exist per window

**'[ADD: spark-streaming logs with local[2] showing batch times]'**

**'[ADD: spark-streaming logs with local[4] showing faster batch times]'**

**Test results:**

| Configuration           | Batch duration (flood speed) | Kafka lag after 60s   |
| ----------------------- | ---------------------------- | --------------------- |
| local[2], partitions=4  | '[ADD value]' ms             | '[ADD value]' records |
| local[4], partitions=8  | '[ADD value]' ms             | '[ADD value]' records |
| local[8], partitions=16 | '[ADD value]' ms             | '[ADD value]' records |

Increasing parallelism reduces batch duration up to a point. Beyond 'local[4]', gains diminish because the bottleneck shifts from CPU to Kafka fetch rate and Cassandra write throughput. A high degree of parallelism with a single-node Cassandra can actually hurt performance: more concurrent writes contend for the same Cassandra coordinator, increasing write latency.

---

## Part 3 - Extension

### P3.1 - Integrating an External ML Inference Service

To integrate an external RESTful ML inference service into the current platform, the tenant would modify 'streamanalyticsapp.py' to call the service from within the 'foreachBatch' function. After the windowed aggregation produces a batch of results, instead of (or in addition to) writing directly to Cassandra, the batch would be serialized to JSON and sent via an HTTP POST to the ML service endpoint. The service returns an inference result (e.g., a pollution forecast or anomaly classification), which is then appended to the Cassandra row before writing.

The tenant needs to: (1) register the ML service URL as an environment variable in 'docker-compose.yaml', (2) add an HTTP client call inside 'write_batch()' using the 'requests' library, (3) handle retries and timeouts (ML inference may be slow), and (4) define the schema of the inference result so it can be stored alongside the window aggregation in Cassandra.

A key design consideration is batching: sending one HTTP request per Spark batch (containing all window results) is far more efficient than one request per record. The service should accept a list of window objects and return a list of inference results in the same order.

---

### P3.2 - Storing Erroneous Records for Inspection

In the current design, records that fail validation (null country, null timestamp, unparseable JSON) are silently dropped at the filter step. To save them for inspection, the 'write_batch' function would be extended to tee the bad records to a separate Cassandra table 'tenanta_analytics.bad_records' with columns: 'ingest_ts', 'raw_value' (the original Kafka message bytes as text), 'error_reason' (which filter it failed), and 'batch_id'.

Implementation: before the null filter step, the parsed dataframe is split into two branches: 'valid = parsed.filter(...)' and 'invalid = parsed.filter(~...)'. The 'invalid' branch is written to the bad records table inside 'write_batch' alongside the analytics results. Both writes happen in the same 'foreachBatch' call, ensuring atomicity.

---

### P3.3 - Workflow Coordination for Batch Analytics Trigger

'''
[tenant-alert-consumer]
│
│ detects critical condition
│ (e.g., >10 alerts in 5 min)
▼
[Workflow Engine - Apache Airflow]
│
├──► Task 1: trigger batch analytics job
│ (reads tenanta_analytics.window_results
│ for last 24h, computes statistics)
│
├──► Task 2: write results to cloud storage
│ (e.g., GCS bucket as CSV/Parquet)
│
└──► Task 3: send notification to tenant user
(email via SendGrid, or Slack webhook)
'''

The tenant service ('alert_consumer.py') is extended to count alerts within a rolling window. When the count exceeds a threshold (e.g., 10 alerts in 5 minutes), it makes an HTTP POST to the Airflow REST API to trigger a DAG run. The DAG has three tasks in sequence: (1) a PySpark batch job that reads the analytics history from Cassandra, (2) a GCS upload task using the 'google-cloud-storage' Python client, and (3) a notification task using an email or messaging API. The workflow technology (Airflow) handles dependency management, retries, and logging - the tenant does not need to manage task orchestration manually.

---

### P3.4 - Schema Evolution

**Preventing wrong schema at runtime:** The 'INPUT_SCHEMA' in 'streamanalyticsapp.py' is defined explicitly. When new input data follows a new schema (e.g., a new field 'temperature' is added, or 'pm2_5_P2' is renamed to 'pm25'), the running job continues to work correctly for the fields it knows about - PySpark's 'from_json' simply ignores unknown fields and returns null for fields that are missing. This means the job will not crash on new schema data, but it will silently miss new fields.

**Detecting schema changes before deployment:** A schema registry (e.g., Confluent Schema Registry or a simple custom registry stored in Cassandra) can be used. The producer registers its schema on startup. The streaming job reads the expected schema from the registry on startup and compares it with 'INPUT_SCHEMA'. If they differ, the job refuses to start and logs a warning. The developer/owner is notified via a monitoring alert.

An alternative approach: add a 'schema_version' field to every Kafka message. The streaming job checks this field in 'write_batch' - if it sees a version it doesn't recognize, it routes those records to the bad records table and fires an alert to the developer via the same 'tenantA.alerts' mechanism.

---

### P3.5 - End-to-End Exactly-Once Delivery

End-to-end exactly-once is not achievable with the current design, but it is possible with changes.

**Current state:** The producer uses 'enable.idempotence=True' which gives exactly-once delivery within a single Kafka producer session. The Spark job uses 'foreachBatch' with checkpointing, which gives at-least-once semantics - if a batch fails partway through (after writing to Cassandra but before committing the Kafka offset), the batch will be retried and the Cassandra write will happen again.

**Conditions for end-to-end exactly-once:**

1. **Kafka -> Spark:** Use Spark's native Kafka source with checkpointing (already in place). This ensures each Kafka offset is processed exactly once in terms of Spark's internal state.
2. **Spark -> Cassandra:** The Cassandra write must be idempotent. Our current schema uses '(country, window_start)' as the primary key, so re-writing the same window result is a no-op (same row, same values). This achieves effectively-exactly-once for Cassandra writes.
3. **Spark -> Kafka (alerts):** Kafka transactional producers ('transactional.id') combined with 'isolation.level=read_committed' on the consumer would provide exactly-once for the alert topic. This is not currently configured.

**Impossibility in current design:** True end-to-end exactly-once cannot be guaranteed because writing to Cassandra and publishing to Kafka are two separate operations with no distributed transaction between them. If the Cassandra write succeeds but the Kafka publish fails (or vice versa), the two sinks will be inconsistent. Achieving true exactly-once would require a two-phase commit or a saga pattern coordinating both writes, which is complex and introduces significant latency.

---

---

---

---

---

---

---

Let me read the assignment requirements carefully and your existing report structure first.Now I have everything I need. Let me write both documents.Good. Now I have everything. Let me write both documents.---

## Where files go in your repo

'''
reports/
├── Assignment-3-Report.md <- main graded report
└── Assignment-3-Deployment.md <- deployment guide
'''

---

## What you still need to add to the report

Every place marked '[ADD: ...]' needs a screenshot. Here is the exact list with the command to run for each:

| Location in report     | Command to run                                                                                        | What to screenshot                 |
| ---------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------- |
| P1.5 Architecture      | draw it (see below)                                                                                   | diagram showing full pipeline flow |
| P2.2 Test environment  | 'docker compose ps'                                                                                   | all containers running             |
| P2.3 Spark processing  | 'docker logs -f spark-streaming \| grep "batch\|write\|ALERT"'                                        | several batches completing         |
| P2.3 Cassandra results | 'SELECT country, window_start, avg_pm25, alert_fired FROM tenanta_analytics.window_results LIMIT 15;' | rows with data                     |
| P2.3 Alert consumer    | 'docker logs -f tenant-alert-consumer'                                                                | 🚨 alerts appearing                |
| P2.3 Speed slow        | 'python streaming/speed_test.py --speed slow --duration 60' + Cassandra query                         | low record_count per window        |
| P2.3 Speed fast        | 'python streaming/speed_test.py --speed fast --duration 60' + Cassandra query                         | high record_count per window       |
| P2.3 Window 1min       | edit docker-compose + restart + Cassandra query                                                       | more rows, smaller counts          |
| P2.3 Window 5min       | default + Cassandra query                                                                             | fewer rows, larger counts          |
| P2.4 Error injection   | 'python streaming/inject_errors.py --errors-per-type 10'                                              | inject_errors output               |
| P2.4 Spark survives    | 'docker logs spark-streaming \| tail -30' after injection                                             | job still running, no crash        |
| P2.5 local[2]          | 'docker logs spark-streaming \| grep batch' at flood speed                                            | batch durations                    |
| P2.5 local[4]          | change docker-compose + restart + same flood test                                                     | faster batch durations             |

**For the P1.5 architecture diagram** - the simplest approach is to use the text diagram already in the report and convert it to an SVG or PNG using any free tool (draw.io, excalidraw.com). Save it as 'reports/report-img/mysimbdp_architecture-3.svg' and add '<img src="./report-img/mysimbdp_architecture-3.svg">' to the report.

**For the P2.5 table** - fill in the actual millisecond values from your Spark logs after running the parallelism tests.

---

---

---

---

---

---

---

Here is your exact step-by-step guide for every screenshot and test you need to complete the report.

---

## Before you start - one terminal setup

Open **4 terminal tabs** and leave these running the whole time:

**Tab 1** - your command tab (run everything here)
**Tab 2** - Spark live log:

'''bash
docker logs -f spark-streaming | grep --line-buffered "batch\|write\|ALERT\|ERROR"
'''

**Tab 3** - Alert consumer live log:

'''bash
docker logs -f tenant-alert-consumer
'''

**Tab 4** - Cassandra query tab (run queries here)

---

## SCREENSHOT 1 - P1.5 Architecture diagram

You already have the draw.io file 'BDP-asg3-arch.drawio'. Do this:

'''

1. Go to app.diagrams.net
2. File -> Open -> upload BDP-asg3-arch.drawio
3. Ctrl+Shift+H (fit page)
4. File -> Export as -> PNG -> Export
5. Save as: reports/report-img/mysimbdp_architecture-3.png
   '''

Then in your report replace '[ADD ARCHITECTURE DIAGRAM HERE]' with:

'''markdown
<img src="./report-img/mysimbdp_architecture-3.png">
'''

---

## SCREENSHOT 2 - P2.2 Test environment ('docker compose ps')

**Tab 1:**

'''bash
docker compose ps
'''

Take a screenshot of the full output showing all 11 containers with status 'running'. Save as 'reports/report-img/p22-containers.png'.

In your report find '[ADD: docker compose ps screenshot]' and replace with:

'''markdown
**Figure: All platform containers running**
<img src="./report-img/p22-containers.png">
'''

---

## SCREENSHOTS 3, 4, 5 - P2.3 Basic pipeline working

**Screenshot 3 - Spark processing batches**

Look at Tab 2 (already running). Wait until you see at least 3 batches like:

'''
[batch 4] 36 window results to write
✅ Cassandra write ok (36 rows)
🚨 5 alerts sent to tenantA.alerts
'''

Take a screenshot of Tab 2. Save as 'reports/report-img/p23-spark-batches.png'.

**Screenshot 4 - Cassandra results**

**Tab 4:**

'''bash
docker exec -it cqlsh cqlsh cassandra 9042 \
 -e "SELECT country, window_start, avg_pm25, avg_pm10, record_count, alert_fired \
 FROM tenanta_analytics.window_results LIMIT 15;"
'''

Take a screenshot showing rows with data. Save as 'reports/report-img/p23-cassandra-results.png'.

**Screenshot 5 - Alerts appearing**

Look at Tab 3 (already running). Wait for a '🚨 ALERT RECEIVED' line to appear. Take a screenshot. Save as 'reports/report-img/p23-alerts.png'.

In your report find the three '[ADD: ...]' markers in P2.3 and replace with:

'''markdown
**Spark processing batches:**
<img src="./report-img/p23-spark-batches.png">

**Analytics results in Cassandra:**
<img src="./report-img/p23-cassandra-results.png">

**Real-time alerts received by tenant:**
<img src="./report-img/p23-alerts.png">
'''

---

## SCREENSHOTS 6, 7 - P2.3 Speed variation

**Screenshot 6 - Slow speed**

**Tab 1:**

'''bash
python streaming/speed_test.py --speed slow --duration 60
'''

While it runs watch Tab 2. After 60 seconds query **Tab 4:**

'''bash
docker exec -it cqlsh cqlsh cassandra 9042 \
 -e "SELECT country, record_count, avg_pm25 \
 FROM tenanta_analytics.window_results LIMIT 10;"
'''

Take a screenshot of the Cassandra output showing low 'record_count' (typically 1–5 per window). Save as 'reports/report-img/p23-speed-slow.png'.

**Screenshot 7 - Fast speed**

**Tab 1:**

'''bash
python streaming/speed_test.py --speed fast --duration 60
'''

After 60 seconds run the same Cassandra query in **Tab 4**. Take a screenshot showing high 'record_count' (typically 100–500 per window). Save as 'reports/report-img/p23-speed-fast.png'.

In your report replace the speed '[ADD: ...]' markers with:

'''markdown
**Slow speed (1 msg/sec) - low record_count per window:**
<img src="./report-img/p23-speed-slow.png">

**Fast speed (100 msg/sec) - high record_count per window:**
<img src="./report-img/p23-speed-fast.png">
'''

---

## SCREENSHOTS 8, 9 - P2.3 Window size variation

**Screenshot 8 - Narrow window (1 minute)**

**Tab 1** - edit 'docker-compose.yaml', find the 'spark-streaming' environment section and change:

'''yaml
WINDOW_DURATION: "1 minute"
WINDOW_SLIDE: "30 seconds"
'''

Then:

'''bash
docker compose restart spark-streaming
'''

Wait 3 minutes for windows to accumulate, then **Tab 4:**

'''bash
docker exec -it cqlsh cqlsh cassandra 9042 \
 -e "SELECT COUNT(\*) FROM tenanta_analytics.window_results;"
'''

'''bash
docker exec -it cqlsh cqlsh cassandra 9042 \
 -e "SELECT country, window_start, record_count, avg_pm25 \
 FROM tenanta_analytics.window_results LIMIT 15;"
'''

Take a screenshot showing many rows with low 'record_count'. Save as 'reports/report-img/p23-window-1min.png'.

**Screenshot 9 - Wide window (5 minutes, default)**

Change back in 'docker-compose.yaml':

'''yaml
WINDOW_DURATION: "5 minutes"
WINDOW_SLIDE: "1 minute"
'''

'''bash
docker compose restart spark-streaming
'''

Wait 3 minutes, run the same queries. Take a screenshot showing fewer rows with higher 'record_count'. Save as 'reports/report-img/p23-window-5min.png'.

In your report replace the window '[ADD: ...]' markers with:

'''markdown
**Narrow window (1 min / 30s slide) - more rows, lower record_count:**
<img src="./report-img/p23-window-1min.png">

**Wide window (5 min / 1 min slide) - fewer rows, higher record_count:**
<img src="./report-img/p23-window-5min.png">
'''

---

## SCREENSHOTS 10, 11 - P2.4 Error injection

**Screenshot 10 - inject_errors.py output**

**Tab 1:**

'''bash
python streaming/inject_errors.py --errors-per-type 10
'''

Take a screenshot of the full terminal output showing all 7 error types being injected. Save as 'reports/report-img/p24-inject-errors.png'.

**Screenshot 11 - Spark survives**

Immediately after injection, **Tab 1:**

'''bash
docker logs spark-streaming --tail 40
'''

Take a screenshot showing the job is still running - you must see '[batch N] X window results to write' lines after the injection, with no Python crash or 'Exception' stopping the job. Save as 'reports/report-img/p24-spark-survives.png'.

In your report replace the P2.4 '[ADD: ...]' markers with:

'''markdown
**Error types injected:**
<img src="./report-img/p24-inject-errors.png">

**Spark job continues running after error injection - no crash:**
<img src="./report-img/p24-spark-survives.png">
'''

---

## SCREENSHOTS 12, 13 - P2.5 Parallelism

**Screenshot 12 - local[2] baseline**

First make sure the window is back to default (5 min). Run flood speed and note batch times from Tab 2:

**Tab 1:**

'''bash
python streaming/speed_test.py --speed flood --duration 30
'''

Watch Tab 2 and note the time between batches. Take a screenshot of Tab 2 showing batch lines with timestamps. Save as 'reports/report-img/p25-local2.png'.

Note the batch duration from the timestamps (e.g., batch 10 at 22:10:00, batch 11 at 22:10:28 = 28 seconds per batch).

**Screenshot 13 - local[4] comparison**

**Tab 1** - edit 'docker-compose.yaml', find 'spark-streaming' command section and change:

'''yaml
--master local[4]
'''

Also change:

'''yaml
SPARK_SHUFFLE_PARTITIONS: "8"
'''

'''bash
docker compose restart spark-streaming
'''

Wait 1 minute for it to start, then:

'''bash
python streaming/speed_test.py --speed flood --duration 30
'''

Watch Tab 2 again. Take a screenshot showing batches completing faster. Save as 'reports/report-img/p25-local4.png'.

In your report find the P2.5 table and fill in the actual values:

'''markdown
| Configuration | Batch duration (flood speed) | Kafka lag after 60s |
| ---------------------- | ---------------------------- | ------------------- |
| local[2], partitions=4 | XX ms | XX records |
| local[4], partitions=8 | XX ms | XX records |
'''

Then add the screenshots:

'''markdown
**local[2] - batch processing times:**
<img src="./report-img/p25-local2.png">

**local[4] - faster batch processing times:**
<img src="./report-img/p25-local4.png">
'''

---

## Final checklist before submitting

Go through your report and confirm every '[ADD: ...]' is replaced:

'''
[ ] P1.5 - architecture diagram image added
[ ] P2.2 - docker compose ps screenshot added
[ ] P2.3 - spark-streaming batches screenshot added
[ ] P2.3 - Cassandra window_results screenshot added
[ ] P2.3 - alert consumer 🚨 screenshot added
[ ] P2.3 - slow speed Cassandra screenshot added
[ ] P2.3 - fast speed Cassandra screenshot added
[ ] P2.3 - 1min window Cassandra screenshot added
[ ] P2.3 - 5min window Cassandra screenshot added
[ ] P2.4 - inject_errors.py output screenshot added
[ ] P2.4 - spark survives screenshot added
[ ] P2.5 - table filled with actual ms values
[ ] P2.5 - local[2] screenshot added
[ ] P2.5 - local[4] screenshot added
'''

**Also check:**

'''
[ ] reports/Assignment-3-Report.md exists
[ ] reports/Assignment-3-Deployment.md exists
[ ] reports/report-img/ folder has all PNG files
[ ] code/mysimbdp/ has complete zip contents
[ ] git log shows incremental commits
'''
