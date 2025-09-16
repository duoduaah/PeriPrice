#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
    source .env
fi

# defaults
INGEST_DATE=${INGEST_DATE:-$(date +%F)}
DATASET=${DATASET:-"corporacion_favorita"}
VERSION=${VERSION:-"v1"}
SOURCE=${SOURCE:-"kaggle"}

echo "Ingesting all CSVs in data/ with ingest_date=$INGEST_DATE"

# Upload csvs to landing bucket with metadata
for local_path in data/*.csv; do
    filename=$(basename "$local_path")
    remote_path="gs://${LANDING_BUCKET}/$DATASET/$VERSION/_incoming/${filename}"
    echo "Uploading $filename -> $remote_path with metadata"
    gcloud storage cp "$local_path" "$remote_path" \
        --custom-metadata="source=$SOURCE,dataset=$DATASET,version=$VERSION"
done

# Copy to raw bucket with ingest_date folders and logical table mapping
for local_path in data/*.csv; do
  filename=$(basename "$local_path")
  source_path="gs://${LANDING_BUCKET}/$DATASET/$VERSION/_incoming/${filename}"

  # Map logical tables
  case "$filename" in
    train.csv|test.csv)
      table_dir="sales"
      ;;
    transactions.csv)
      table_dir="transactions"
      ;;
    oil.csv)
      table_dir="oil_prices"
      ;;
    holidays_events.csv)
      table_dir="holidays"
      ;;
    stores.csv)
      table_dir="stores"
      ;;
    sample_submission.csv)
      table_dir="_docs"
      ;;
    *)
      table_dir="misc"
      ;;
  esac

  raw_path="gs://${RAW_BUCKET}/$DATASET/$VERSION/${table_dir}/ingest_date=${INGEST_DATE}/${filename}"
  echo "Copying $filename from landing -> raw ($table_dir)"
  gcloud storage cp "$source_path" "$raw_path"
done

echo " All CSVs ingested successfully!"