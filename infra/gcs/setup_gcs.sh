#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
    source .env
else
    echo " No .env file found. Using defaults."
fi

# Default values (can be overridden in .env)
PROJECT_ID=${PROJECT_ID:-"your-gcp-project-id"}
REGION=${REGION:-"your-region"}
USER_TAG=${USER_TAG:-"your-user"}


# --- Lifecycle JSONs ---
cat > landing-lifecycle.json <<'JSON'
{
  "rule": [
    { "action": {"type": "Delete"}, "condition": {"age": 7} }
  ]
}
JSON

cat > raw-lifecycle.json <<'JSON'
{
  "rule": [
    { "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"}, "condition": {"age": 90} }
  ]
}
JSON


echo "Setting up GCS buckets in project: $PROJECT_ID ($REGION)"

# Create Landing bucket
gcloud storage buckets create gs://$LANDING_BUCKET \
  --project=$PROJECT_ID \
  --location=$REGION \
  --uniform-bucket-level-access || echo "Bucket $LANDING_BUCKET already exists."

# Enable versioning and set lifecycle: delete objects older than 7 days
gcloud storage buckets update gs://$LANDING_BUCKET --versioning --lifecycle-file=landing-lifecycle.json



# Create Raw bucket
gcloud storage buckets create gs://$RAW_BUCKET \
  --project=$PROJECT_ID \
  --location=$REGION \
  --uniform-bucket-level-access 

# Enable versioning and set lifecycle: move objects to Nearline after 90 days
gcloud storage buckets update gs://$RAW_BUCKET --versioning --lifecycle-file=raw-lifecycle.json


rm landing-lifecycle.json raw-lifecycle.json

echo "Buckets set up successfully!"
echo "Landing: gs://$LANDING_BUCKET"
echo "Raw:     gs://$RAW_BUCKET"