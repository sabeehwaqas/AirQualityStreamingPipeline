=================================================================
GCS BUCKET SETUP GUIDE
For mysimbdp silver pipeline caching (P2.4)
=================================================================

## STEP 1 — Install Google Cloud SDK (if not already installed)

On Mac:
brew install --cask google-cloud-sdk

On Linux:
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

Verify:
gcloud --version

## STEP 2 — Login to Google Cloud

gcloud auth login

# This opens a browser. Log in with your Aalto/Google account.

Also set application default credentials (needed by Python SDK):
gcloud auth application-default login

## STEP 3 — Create a GCS Bucket

# Replace YOUR_PROJECT_ID with your actual GCP project ID

# Replace YOUR_BUCKET_NAME with something unique e.g. mysimbdp-cache-2026

gcloud config set project YOUR_PROJECT_ID

gcloud storage buckets create gs://YOUR_BUCKET_NAME \
 --location=europe-north1 \
 --uniform-bucket-level-access

Verify it was created:
gcloud storage buckets list

## STEP 4 — Create folder structure in the bucket

# GCS has no real folders, but we create placeholder objects

# so the paths appear in the console.

echo "" | gcloud storage cp - gs://YOUR_BUCKET_NAME/tenantA/pending/.keep
echo "" | gcloud storage cp - gs://YOUR_BUCKET_NAME/tenantA/processed/.keep
echo "" | gcloud storage cp - gs://YOUR_BUCKET_NAME/tenantB/pending/.keep
echo "" | gcloud storage cp - gs://YOUR_BUCKET_NAME/tenantB/processed/.keep

## STEP 5 — Create a service account key for Docker containers

gcloud iam service-accounts create mysimbdp-sa \
 --display-name="mysimbdp service account"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
 --member="serviceAccount:mysimbdp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
 --role="roles/storage.objectAdmin"

gcloud iam service-accounts keys create ./gcp-key.json \
 --iam-account="mysimbdp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com"

# This creates gcp-key.json in your project folder.

# IMPORTANT: add gcp-key.json to your .gitignore — never commit it.

echo "gcp-key.json" >> .gitignore

## STEP 6 — Update docker-compose.yaml for batchmanager

In docker-compose.yaml, under the batchmanager service, add:

    environment:
      CASSANDRA_HOST: "cassandra"
      TENANT_CONFIGS_DIR: "/app/tenant_configs"
      CACHE_BASE_DIR: "/app/cache"
      BATCH_MANAGER_PORT: "8003"
      GCS_BUCKET: "YOUR_BUCKET_NAME"         # <-- add this
      GOOGLE_APPLICATION_CREDENTIALS: "/app/gcp-key.json"  # <-- add this

    volumes:
      - ./cache:/app/cache
      - ./logs:/app/logs
      - ./gcp-key.json:/app/gcp-key.json     # <-- add this mount

## STEP 7 — Update Dockerfile.platform to copy key

The key is mounted as a volume so no code change to Dockerfile is needed.
The GOOGLE_APPLICATION_CREDENTIALS env var tells the GCS Python SDK where to find it.

## STEP 8 — Rebuild and test

docker compose build --no-cache
docker compose up -d

# Manually trigger a pipeline run and watch GCS

curl -X POST http://localhost:8003/tenants/tenantA/run

# Verify files appear in GCS

gcloud storage ls gs://YOUR_BUCKET_NAME/tenantA/pending/
gcloud storage ls gs://YOUR_BUCKET_NAME/tenantA/processed/

# Check logs (cache_mode should say "local+gcs")

curl http://localhost:8003/tenants/tenantA/logs

## STEP 9 — Compare local vs GCS performance

Run the pipeline in local-only mode (remove GCS_BUCKET from compose), then
with GCS_BUCKET set. Compare the extract_sec values in:

    curl http://localhost:8003/stats

Or in cqlsh:
SELECT run_id, cache_mode, extract_sec, transform_sec, data_size_bytes,
records_loaded, elapsed_sec
FROM platform_logs.silver_pipeline_logs
LIMIT 20;

Expected result: - local: extract_sec ~0.1–0.5s (just Cassandra query + disk write) - local+gcs: extract_sec ~1–5s (Cassandra query + disk write + GCS upload) - transform_sec should be similar in both modes (it reads local file in both cases)

## STEP 10 — View files in GCS Console

https://console.cloud.google.com/storage/browser/YOUR_BUCKET_NAME

=================================================================
QUICK REFERENCE — env vars for the pipeline
=================================================================
GCS_BUCKET = bucket name (no gs:// prefix) e.g. "mysimbdp-cache-2026"
GCS_PREFIX = folder prefix inside bucket default: "tenantA" or "tenantB"
GOOGLE_APPLICATION_CREDENTIALS = path to service account key JSON

When GCS_BUCKET is empty or unset → pipeline runs in local-only mode.
When GCS_BUCKET is set → pipeline runs in local+gcs mode.
=================================================================
