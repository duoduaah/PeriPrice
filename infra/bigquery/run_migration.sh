#!/usr/bin/env bash
set -euo pipefail

# Load env
if [ -f ".env" ]; then
  source .env
else
  echo "Missing .env. Copy .env.example to .env and fill values."; exit 1
fi

# Basic checks
: "${PROJECT_ID:?Set in .env}"
: "${BQ_LOCATION:?Set in .env}"
: "${EXT_DATASET:?Set in .env}"
: "${RAW_NATIVE_DATASET:?Set in .env}"
: "${CURATED_DATASET:?Set in .env}"

gcloud config set project "$PROJECT_ID" >/dev/null

# Helper to substitute @VARS inside SQL before piping to bq
render() {
  sed \
    -e "s/@PROJECT/$PROJECT_ID/g" \
    -e "s/@LOCATION/$BQ_LOCATION/g" \
    -e "s/@RAW_EXT_DATASET/$EXT_DATASET/g" \
    -e "s/@RAW_NATIVE_DATASET/$RAW_NATIVE_DATASET/g" \
    -e "s/@CURATED_DATASET/$CURATED_DATASET/g"
}

echo "Creating datasets (if needed)…"
render < infra/bigquery/datasets.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Migrating sales…"
render < infra/bigquery/migration/01_sales_to_native.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Migrating items…"
render < infra/bigquery/migration/02_items_to_native.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Migrating stores…"
render < infra/bigquery/migration/03_stores_to_native.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Migrating holidays…"
render < infra/bigquery/migration/04_holidays_to_native.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Migrating oil prices…"
render < infra/bigquery/migration/05_oil_prices_to_native.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Migrating transactions…"
render < infra/bigquery/migration/06_transactions_to_native.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "Done."
