CREATE OR REPLACE TABLE `@PROJECT.@RAW_NATIVE_DATASET.holidays` 
PARTITION BY date AS
SELECT
    DATE(date)                      AS date,
    CAST(type AS STRING)            AS type,
    CAST(locale AS STRING)          AS locale,
    CAST(locale_name AS STRING)      AS locale_name,
    CAST(description AS STRING)     AS description,
    CASE
        WHEN LOWER(CAST(transferred AS STRING)) IN ('true', '1', 'yes') THEN TRUE 
        WHEN LOWER(CAST(transferred AS STRING)) IN ('false', '0', 'no') THEN FALSE 
        ELSE NULL
    END As transferred
FROM `@PROJECT.@RAW_EXT_DATASET.holidays_ext`;

