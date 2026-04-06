# mysimbdp Assignment 3 - Deployment Guide

## Folder structure

```
code/mysimbdp/
├── docker-compose.yaml          ← all services (Asg2 + Asg3)
├── bootstrap.sh                 ← one-command setup script
├── Dockerfile.platform          ← platform image (all Python services)
├── Dockerfile.producer          ← tenanta-producer image
├── Dockerfile.spark             ← PySpark streaming image
├── requirements.platform.txt    ← Python deps for platform image
├── requirements.spark.txt       ← Python deps for Spark image
├── platform/
│   ├── streamingestmanager.py
│   ├── streamingestmonitor.py
│   └── batchmanager.py
├── streaming/
│   ├── streamanalyticsapp.py    ← PySpark streaming job (NEW Asg3)
│   ├── inject_errors.py         ← error injection test (NEW Asg3)
│   └── speed_test.py            ← speed test producer (NEW Asg3)
└── tenants/
    ├── tenantA/
    │   ├── tenantA_producer.py  ← patched for Docker env vars
    │   ├── alert_consumer.py    ← alert receiver (NEW Asg3)
    │   ├── streamingestworker.py
    │   ├── silverpipeline_tenantA.py
    │   └── tenantA.yaml
    └── tenantB/
        ├── tenantB_producer.py
        ├── streamingestworker.py
        ├── silverpipeline_tenantB.py
        └── tenantB.yaml
```

---

## Prerequisites (one time only)

Docker Desktop must be installed and running.

```bash
# Make scripts executable
chmod +x bootstrap.sh

# Python deps on host machine (only needed for speed_test.py and inject_errors.py)
pip install confluent-kafka requests
```

---

## Every fresh start - full sequence

### Step 1 - Clean wipe

Always do this before a fresh start to avoid stale schema/data issues:

```bash
cd code/mysimbdp
docker compose down -v
docker rmi mysimbdp-platform:latest 2>/dev/null || true
```

### Step 2 - Bootstrap (does everything)

```bash
./bootstrap.sh
```

This script does the following automatically:

1. Builds all Docker images (`mysimbdp-platform`, `tenanta-producer`, `spark-streaming`)
2. Starts all containers
3. Waits for Kafka to be ready
4. Creates Kafka topics: `tenantA.bronze.raw`, `tenantB.bronze.raw`, `tenantA.alerts`
5. Waits for Cassandra to be ready (takes 2–4 minutes)
6. Creates all Cassandra keyspaces and tables (Asg2 + Asg3 schemas)
7. Auto-starts `streamingestworker` for tenantA and tenantB via API call

Wait for `✅ Bootstrap complete.` - takes 5–10 minutes on first run (image downloads).

### Step 3 - Verify all containers are running

```bash
docker compose ps
```

Expected output - all containers `running`:

```
NAME                    STATUS
broker                  running
cassandra               running
cqlsh                   running
streamingestmanager     running
streamingestmonitor     running
batchmanager            running
tenanta-producer        running
spark-master            running
spark-worker            running
spark-streaming         running
tenant-alert-consumer   running
```

### Step 4 - Verify pipeline is flowing

```bash
# Confirm Kafka is receiving sensor data
docker exec -it broker bash -lc \
  "/opt/kafka/bin/kafka-console-consumer.sh \
   --bootstrap-server localhost:9092 \
   --topic tenantA.bronze.raw --max-messages 3"

# Watch Spark processing batches (wait ~2 min for first batch)
docker logs -f spark-streaming | grep "batch\|write\|ALERT"

# Watch alerts firing in real time
docker logs -f tenant-alert-consumer

# Query analytics results in Cassandra
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "SELECT country, window_start, avg_pm25, avg_pm10, alert_fired \
      FROM tenanta_analytics.window_results LIMIT 15;"
```

---

## Running the assignment tests

### Part 2 Q3 - Speed tests (run from host machine)

```bash
# Slow - 1 msg/sec for 60 seconds
python streaming/speed_test.py --speed slow --duration 60

# Medium - 10 msg/sec
python streaming/speed_test.py --speed medium --duration 60

# Fast - 100 msg/sec
python streaming/speed_test.py --speed fast --duration 60

# Flood - max speed
python streaming/speed_test.py --speed flood --duration 30
```

After each test, query Cassandra to observe record_count differences:

```bash
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "SELECT country, record_count, avg_pm25 \
      FROM tenanta_analytics.window_results LIMIT 20;"
```

### Part 2 Q3 - Window parameter change

Edit `docker-compose.yaml` under `spark-streaming` environment:

```yaml
# Narrow window (more rows, lower record_count)
WINDOW_DURATION: "1 minute"
WINDOW_SLIDE:    "30 seconds"

# Wide window (fewer rows, higher record_count)
WINDOW_DURATION: "10 minutes"
WINDOW_SLIDE:    "2 minutes"
```

Then restart just Spark:

```bash
docker compose restart spark-streaming
```

### Part 2 Q4 - Error injection

```bash
# Inject 10 of each error type
python streaming/inject_errors.py --errors-per-type 10

# Watch Spark survive without crashing
docker logs -f spark-streaming | grep "batch\|write\|ERROR"
```

### Part 2 Q5 - Parallelism tests

Edit `docker-compose.yaml` under `spark-streaming`:

```yaml
# Increase parallelism
SPARK_SHUFFLE_PARTITIONS: "8"
```

And in the `command` section:

```yaml
# Change local[2] to local[4]
--master local[4]
```

Then restart:

```bash
docker compose restart spark-streaming

# Run flood test and compare batch times
python streaming/speed_test.py --speed flood --duration 30
docker logs -f spark-streaming | grep "batch"
```

---

## Useful commands

```bash
# See all container statuses
docker compose ps

# Stream Spark logs
docker logs -f spark-streaming

# Stream alert consumer logs
docker logs -f tenant-alert-consumer

# Stream producer logs
docker logs -f tenanta-producer

# Query all analytics results
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "SELECT country, window_start, avg_pm25, alert_fired \
      FROM tenanta_analytics.window_results LIMIT 30;"

# Query only alert windows
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "SELECT country, window_start, avg_pm25, avg_pm10 \
      FROM tenanta_analytics.window_results \
      WHERE alert_fired = True ALLOW FILTERING LIMIT 20;"

# Count total windows stored
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "SELECT COUNT(*) FROM tenanta_analytics.window_results;"

# Check bronze records (Asg2 worker still running)
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "SELECT sensor_id, ingest_ts FROM tenanta_bronze.records LIMIT 5;"

# Check Kafka topics
docker exec -it broker bash -lc \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"

# Check consumer group lag
docker exec -it broker bash -lc \
  "/opt/kafka/bin/kafka-consumer-groups.sh \
   --bootstrap-server localhost:9092 \
   --describe --group streamanalyticsapp-tenantA"

# Spark Web UI
open http://localhost:8080
```

---

## Stop and restart

```bash
# Stop all containers, keep data
docker compose down

# Stop all containers AND wipe all data (use for fresh restart)
docker compose down -v

# Restart only Spark (after changing window params)
docker compose restart spark-streaming

# Restart only the alert consumer
docker compose restart tenant-alert-consumer
```

---

## Common problems

**`mysimbdp-platform:latest already exists` error during bootstrap**

Multiple services tried to build the same image simultaneously. Fix:

```bash
docker rmi -f mysimbdp-platform:latest
docker compose build --no-cache streamingestmanager
./bootstrap.sh
```

**`spark-streaming` keeps restarting**

On first run, Spark downloads ~500MB of JARs. Check:

```bash
docker logs spark-streaming | tail -30
```

If you see `Downloading` lines, just wait. If you see a Python error, check the `streamanalyticsapp.py` path inside the container:

```bash
docker exec -it spark-streaming ls /app/
```

**`tenant-alert-consumer` shows connection refused**

The old image is cached. Force rebuild:

```bash
docker rmi -f mysimbdp-platform:latest
docker compose build --no-cache streamingestmanager
docker compose up -d --force-recreate tenant-alert-consumer
```

**No rows in `tenanta_analytics.window_results` after 5 minutes**

Check Spark logs for Cassandra write errors:

```bash
docker logs spark-streaming | grep "Cassandra\|ERROR\|batch"
```

If you see `Couldn't find keyspace`, the keyspace name is wrong. Verify:

```bash
docker exec -it cqlsh cqlsh cassandra 9042 -e "DESCRIBE KEYSPACES;"
```

Should show `tenanta_analytics` (all lowercase).

**Workers not started (no bronze records)**

Bootstrap auto-starts them, but if they failed:

```bash
curl -X POST http://localhost:8001/tenants/tenantA/workers/start \
     -H 'Content-Type: application/json' -d '{}'
curl -X POST http://localhost:8001/tenants/tenantB/workers/start \
     -H 'Content-Type: application/json' -d '{}'
```

**Silver pipeline shows `no_new_data`**

Reset the watermark:

```bash
docker exec -it cqlsh cqlsh cassandra 9042 \
  -e "DELETE FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantA';"
```
