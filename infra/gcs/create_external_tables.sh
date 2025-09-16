#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
    source .env
else
    echo ".env file not found!"
    exit 1
fi

echo "Using project: $PROJECT_ID"
echo "Dataset: $EXT_DATASET"
echo "Raw bucket: $RAW_BUCKET"



# Create dataset if it doesn’t exist

if ! bq --project_id=$PROJECT_ID show --dataset $EXT_DATASET > /dev/null 2>&1; then
    echo "Creating dataset $EXT_DATASET"
    bq --location=$REGION --project_id=$PROJECT_ID mk $PROJECT_ID:$EXT_DATASET
else
    echo "Dataset $EXT_DATASET already exists, skipping creation"
fi



# External table: holidays
bq mk --external_table_definition=CSV=\
"gs://$RAW_BUCKET/$DATASET/$VERSION/holidays/ingest_date=${INGEST_DATE}/*.csv" \
$PROJECT_ID:$EXT_DATASET.holidays_ext

# External table: items
bq mk --external_table_definition=CSV=\
"gs://$RAW_BUCKET/$DATASET/$VERSION/misc/ingest_date=${INGEST_DATE}/*.csv" \
$PROJECT_ID:$EXT_DATASET.items_ext

# External table: oil_prices
bq mk --external_table_definition=CSV=\
"gs://$RAW_BUCKET/$DATASET/$VERSION/oil_prices/ingest_date=${INGEST_DATE}/*.csv" \
$PROJECT_ID:$EXT_DATASET.oil_prices_ext


# External table: sales
bq mk --external_table_definition=CSV=\
"gs://$RAW_BUCKET/$DATASET/$VERSION/sales/ingest_date=${INGEST_DATE}/train.csv" \
$PROJECT_ID:$EXT_DATASET.sales_ext

# External table: stores
bq mk --external_table_definition=CSV=\
"gs://$RAW_BUCKET/$DATASET/$VERSION/stores/ingest_date=${INGEST_DATE}/*.csv" \
$PROJECT_ID:$EXT_DATASET.stores_ext

# External table: transactions
bq mk --external_table_definition=CSV=\
"gs://$RAW_BUCKET/$DATASET/$VERSION/transactions/ingest_date=${INGEST_DATE}/*.csv" \
$PROJECT_ID:$EXT_DATASET.transactions_ext



echo "External tables created in dataset $EXT_DATASET"