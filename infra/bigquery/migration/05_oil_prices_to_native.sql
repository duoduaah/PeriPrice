CREATE OR REPLACE TABLE `@PROJECT.@RAW_NATIVE_DATASET.oil_prices` 
PARTITION BY date AS
SELECT
    DATE(date)                          AS date,
    SAFE_CAST(dcoilwtico AS FLOAT64)    AS dcoilwtico
FROM `@PROJECT.@RAW_EXT_DATASET.oil_prices_ext`;