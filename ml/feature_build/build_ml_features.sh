#!/usr/bin/env bash
set -euo pipefail

if [ -f ".env" ]; then
    source .env
else
    echo "missing .env"
fi

: "${PROJECT_ID:?Set in .env}"
: "${CURATED_DATASET:?Set in .env}"
: "${ML_DATASET:?Set in .env}"
: "${BQ_LOCATION:?Set in .env}"

gcloud config set project "$PROJECT_ID" >/dev/null


render() {
  sed \
    -e "s/@PROJECT/$PROJECT_ID/g" \
    -e "s/@LOCATION/$BQ_LOCATION/g" \
    -e "s/@ML_DATASET/$ML_DATASET/g" \
    -e "s/@CURATED_DATASET/$CURATED_DATASET/g" 
}

echo "Creating ml dataset if needed ...."
render < ml/feature_build/dataset.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "features_clean .... "
render < ml/feature_build/features_clean.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "features_enriched .... "
render < ml/feature_build/features_enriched.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false

echo "features_split .... "
render < ml/feature_build/features_split.sql | bq query --location="$BQ_LOCATION" --use_legacy_sql=false


echo "DONE!!!"


