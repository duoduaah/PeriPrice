CREATE OR REPLACE TABLE `@PROJECT.@ML_DATASET.features_clean` AS
WITH src AS (
  SELECT
    DATE AS date,

    -- IDs
    CAST(store_nbr AS STRING) AS store_nbr,
    CAST(item_nbr  AS STRING) AS item_nbr,
    CAST(class     AS STRING) AS class,
    CAST(cluster   AS STRING) AS cluster,
    CAST(family    AS STRING) AS family,

    -- Label
    SAFE_CAST(unit_sales AS FLOAT64) AS unit_sales,
    SAFE_CAST(log_sales  AS FLOAT64) AS log_sales,

    -- Price/expiry
    SAFE_CAST(base_price      AS FLOAT64) AS base_price,
    SAFE_CAST(discount_pct    AS FLOAT64) AS discount_pct,
    SAFE_CAST(effective_price AS FLOAT64) AS effective_price,
    SAFE_CAST(time_to_expiry  AS INT64)   AS time_to_expiry,

    -- Calendar
    SAFE_CAST(dow   AS INT64) AS dow,
    SAFE_CAST(month AS INT64) AS month,
    SAFE_CAST(year  AS INT64) AS year
  FROM `@PROJECT.@CURATED_DATASET.fct_sales_features`
  WHERE perishable_bool = TRUE
    AND base_price > 0
    AND effective_price >= 0
)
SELECT * FROM src
