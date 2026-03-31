Here is the complete workflow explained step by step, every component, every decision, every data flow.

---

## The Scenario

Your streaming pipeline is running normally. Spark is computing 5-minute windows of PM2.5/PM10 averages per country every 30 seconds. Suddenly, Finland (FI) starts showing PM2.5 values above 15 μg/m³ repeatedly — a pollution event is happening. The system needs to:

1. Detect this is not a one-off spike but a sustained critical condition
2. Automatically trigger a deep historical analysis
3. Save the analysis results to cloud storage
4. Notify the tenant user

---

## The Full Flow — Every Step

### Phase 1 — Normal Streaming (already running)

```
sensor.community API
      ↓  every 0.5 seconds
tenanta-producer
      ↓  publishes JSON to
Kafka topic: tenantA.bronze.raw
      ↓  consumed by
spark-streaming (PySpark job)
      ↓  every 30 seconds processes a batch
      ↓  computes avg PM2.5 per country per 5-min window
      ↓  if avg PM2.5 > 15.0 → alert_fired = True
      ↓  writes results to Cassandra (always)
      ↓  publishes to Kafka topic: tenantA.alerts (only when alert_fired=True)
```

This is your normal pipeline. It runs forever without human intervention.

---

### Phase 2 — Alert Received by Tenant Consumer

The `alert_consumer.py` script is subscribed to `tenantA.alerts`. Every time Spark fires an alert it receives a JSON message like this:

```json
{
  "country": "FI",
  "window_start": "2026-03-28 22:10:00",
  "window_end": "2026-03-28 22:15:00",
  "avg_pm25": 22.4,
  "avg_pm10": 41.2,
  "alert_reason": "PM2.5_BREACH",
  "alert_ts": "2026-03-28T22:15:30Z"
}
```

The consumer receives this and does two things simultaneously:

**Thing 1 — Prints the alert** (already working):

```
🚨 ALERT RECEIVED  country=FI  PM2.5=22.4μg/m³  reason=PM2.5_BREACH
```

**Thing 2 — Feeds a rolling counter** (new logic for P3.3):

The consumer maintains an in-memory dictionary that tracks how many alerts have arrived per country in the last 5 minutes:

```python
alert_counts = {
    "FI": [22:10:00, 22:11:00, 22:12:00],   # 3 alerts so far
    "DE": [22:09:00],                          # 1 alert
}
```

Every time a new alert arrives, expired timestamps (older than 5 minutes) are removed and the new timestamp is added. The count is then checked.

---

### Phase 3 — The Decision Point

After each new alert is added to the counter, the consumer checks:

```
Is the count for this country > 10 in the last 5 minutes?
```

**If NO** — do nothing. Keep consuming. The counter keeps growing but has not crossed the threshold yet. This handles the case where a country gets 2–3 alerts which is just normal variation in PM2.5.

**If YES** — a critical condition is detected. This means PM2.5 has been above 15 μg/m³ for more than 10 consecutive 1-minute sliding windows — roughly 10+ minutes of sustained dangerous air quality. This is when the workflow kicks in.

Why 10 as the threshold? Because:

- Each Spark batch runs every 30 seconds
- Windows slide every 1 minute
- 10 alerts in 5 minutes means the problem has persisted for the entire 5-minute window multiple times
- A single spike would produce 1–2 alerts, not 10

---

### Phase 4 — Triggering Airflow

The `alert_consumer.py` makes an HTTP POST request to the Airflow REST API:

```
POST http://airflow:8080/api/v1/dags/air_quality_batch_analytics/dagRuns
Content-Type: application/json

{
  "conf": {
    "country":      "FI",
    "triggered_at": "2026-03-28T22:15:30Z",
    "alert_count":  11,
    "avg_pm25":     22.4
  }
}
```

**What happens in Airflow when it receives this:**

1. Airflow creates a new DAG Run for the `air_quality_batch_analytics` DAG
2. It logs the trigger with the `conf` payload
3. It schedules Task 1 to run immediately
4. The `conf` values are available to all tasks in the DAG as variables

**Immediately after sending the POST**, the consumer resets the counter for Finland to zero:

```python
alert_counts["FI"] = []
```

This prevents the same pollution event from triggering the workflow again in the next 5 minutes. It will only re-trigger if a new sustained event begins.

---

### Phase 5 — Airflow DAG Runs (3 Tasks in Sequence)

Airflow enforces that tasks run in order. Task 2 only starts if Task 1 succeeds. Task 3 only starts if Task 2 succeeds. If any task fails, Airflow retries it N times before marking the DAG run as failed.

---

#### Task 1: `batch_analytics`

**What it does:**

A PySpark batch job (not streaming — this is a one-time run) connects to Cassandra and reads the `tenanta_analytics.window_results` table for the last 24 hours, filtered by the country from the DAG conf:

```sql
SELECT country, window_start, avg_pm25, avg_pm10, record_count, alert_fired
FROM tenanta_analytics.window_results
WHERE country = 'FI'
AND window_start >= '2026-03-27 22:15:00'
ALLOW FILTERING
```

It then computes a statistical summary:

- Average PM2.5 per hour over 24h
- Peak window (highest PM2.5 recorded)
- How many windows had `alert_fired = True`
- How many windows were safe (below threshold)
- Overall trend (improving / worsening / stable)

The result is a small DataFrame that looks like:

```
hour       | avg_pm25 | peak_pm25 | alert_count
10:00–11:00|   8.2    |   11.4    |     0
11:00–12:00|   12.1   |   18.3    |     3
12:00–13:00|   19.4   |   28.1    |     8   ← pollution event
```

This result is held in Spark memory and passed to Task 2 as a file written to a local temp directory.

**What Airflow does if Task 1 fails:**

- Retries up to 3 times with a 5-minute gap between retries
- If all retries fail, marks Task 1 as `failed`, skips Tasks 2 and 3, marks the entire DAG run as `failed`
- Logs the full error so the platform operator can investigate

---

#### Task 2: `upload_results`

**What it does:**

Reads the batch analytics result file produced by Task 1 and uploads it to Google Cloud Storage as a Parquet file:

```
gs://mysimbdp-bucket/batch_reports/FI/2026-03-28/report.parquet
```

The path is constructed from the country and date in the DAG conf so each report has a unique, queryable location. A human or another analytics tool can download this file directly from GCS.

The task also writes a small JSON metadata file alongside it:

```json
{
  "country": "FI",
  "analysis_date": "2026-03-28",
  "triggered_by": "critical_alert",
  "alert_count": 11,
  "peak_pm25": 28.1,
  "gcs_path": "gs://mysimbdp-bucket/batch_reports/FI/2026-03-28/report.parquet"
}
```

**What Airflow does if Task 2 fails:**

- The analytics result from Task 1 is still in local temp storage
- Retries up to 2 times
- If all retries fail, the DAG run is marked `failed` but Task 1's result is not lost — the platform operator can manually re-run Task 2 only using the Airflow UI

---

#### Task 3: `notify_tenant`

**What it does:**

Reads the GCS path from Task 2's output and sends a notification to the tenant user. The notification contains:

- Which country triggered the alert
- The peak PM2.5 value recorded
- How many windows were in breach
- A direct download link to the GCS report file

**Two notification options:**

Option A — Email via SendGrid:

```
Subject: [mysimbdp] Critical Air Quality Alert — Finland
Body:
  A sustained PM2.5 breach was detected for Finland.
  Peak PM2.5: 28.1 μg/m³ (WHO limit: 15.0 μg/m³)
  Breach windows: 11 of 60 in the last 24 hours

  Full analysis report:
  https://storage.googleapis.com/mysimbdp-bucket/batch_reports/FI/2026-03-28/report.parquet
```

Option B — Slack webhook POST:

```json
{
  "text": "🚨 *Critical Air Quality Alert — Finland*\nPeak PM2.5: 28.1 μg/m³\n<https://...|Download Report>"
}
```

**What Airflow does if Task 3 fails:**

- Retries up to 2 times
- If all retries fail, the GCS file is still there — the user can be notified manually
- The DAG run is marked `failed` but the data is safe

---

### Phase 6 — DAG Complete

Airflow marks the entire DAG run as `success`. The full execution log is visible in the Airflow UI showing:

```
DAG: air_quality_batch_analytics
Run ID: manual__2026-03-28T22:15:30
Triggered by: alert_consumer.py via REST API
Conf: {country: FI, alert_count: 11}

Task 1: batch_analytics    → SUCCESS  (ran 47 seconds)
Task 2: upload_results     → SUCCESS  (ran 8 seconds)
Task 3: notify_tenant      → SUCCESS  (ran 2 seconds)

Total duration: 57 seconds
```

---

### Why Airflow Specifically

You could just put all 3 tasks in one Python function inside `alert_consumer.py`. The reason to use a proper workflow engine like Airflow instead is:

**Dependency management** — Airflow guarantees Task 2 never runs if Task 1 failed. If you wrote this in plain Python with `if/else`, a bug could cause Task 2 to run with no data and silently produce a corrupt report.

**Retry handling** — Airflow retries failed tasks automatically with configurable backoff. Writing this yourself is error-prone.

**Visibility** — Every DAG run is logged with start time, end time, which task failed, the error message, and the input conf. Without Airflow you would need to build this logging yourself.

**Separation of concerns** — `alert_consumer.py` only needs to detect the condition and fire the trigger. It does not need to know how the batch analytics works, where GCS is, or how email is sent. Each task is independently testable.

**Scheduling independence** — If the alert consumer crashes and restarts, the already-triggered DAG run keeps running in Airflow independently. The workflow is not tied to the lifetime of the consumer process.

---

## Summary — The Whole Thing in One View

```
[spark-streaming]
  every 30s, batch completes
  avg_pm25 > 15 for FI → alert_fired=True
        ↓ publishes to Kafka

[Kafka: tenantA.alerts]
        ↓ consumed by

[alert_consumer.py]
  receives alert JSON
  adds timestamp to rolling_counter["FI"]
  removes expired timestamps (> 5 min old)

  IF count["FI"] <= 10:
    print 🚨, continue consuming

  IF count["FI"] > 10:
    POST /api/v1/dags/.../dagRuns  → Airflow
    reset counter["FI"] = []
        ↓ triggers

[Airflow DAG: air_quality_batch_analytics]
        ↓
  Task 1: batch_analytics
    read tenanta_analytics.window_results (24h)
    compute hourly PM2.5 stats for FI
    write result to /tmp/batch_result_FI.parquet
    SUCCESS → trigger Task 2
    FAIL → retry 3x → DAG failed
        ↓
  Task 2: upload_results
    read /tmp/batch_result_FI.parquet
    upload to gs://bucket/batch_reports/FI/2026-03-28/
    write metadata JSON
    SUCCESS → trigger Task 3
    FAIL → retry 2x → DAG failed
        ↓
  Task 3: notify_tenant
    build notification with GCS link
    POST to SendGrid or Slack webhook
    SUCCESS → DAG complete ✅
    FAIL → retry 2x → DAG failed (GCS file still safe)
```
