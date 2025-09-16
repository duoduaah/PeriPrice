CREATE OR REPLACE TABLE `@PROJECT.@RAW_NATIVE_DATASET.transactions` 
PARTITION BY date 
CLUSTER BY store_nbr AS
SELECT
    DATE(date)                          AS date,
    CAST(store_nbr AS INT64)            AS store_nbr,
    SAFE_CAST(transactions AS INT64)  AS transactions
FROM `@PROJECT.@RAW_EXT_DATASET.transactions_ext`;