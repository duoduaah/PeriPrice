CREATE OR REPLACE TABLE `@PROJECT.@RAW_NATIVE_DATASET.sales`
PARTITION BY date
CLUSTER BY store_nbr, item_nbr AS
SELECT
    CAST(id AS STRING)                  AS id,
    DATE(date)                          AS date,
    CAST(store_nbr AS INT64)            AS store_nbr,
    CAST(item_nbr AS INT64)             AS item_nbr,
    -- on promotion boolean parse: NULL if unknown
    CASE
        WHEN LOWER(CAST(onpromotion AS STRING)) IN ('true', '1', 'yes') THEN TRUE
        WHEN LOWER(CAST(onpromotion AS STRING)) IN ('false', '0', 'no') THEN FALSE
        ELSE NULL
    END                                 AS onpromotion,
    SAFE_CAST(unit_sales AS FLOAT64)    AS unit_sales
FROM `@PROJECT.@RAW_EXT_DATASET.sales_ext`

