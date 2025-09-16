CREATE OR REPLACE TABLE `@PROJECT.@RAW_NATIVE_DATASET.stores` AS
SELECT
    CAST(store_nbr AS INT64)        AS store_nbr,
    CAST(city AS STRING)            AS city,
    CAST(state AS STRING)           AS state,
    CAST(type AS STRING)            AS type,
    SAFE_CAST(cluster AS INT64)     AS cluster
FROM `@PROJECT.@RAW_EXT_DATASET.stores_ext`