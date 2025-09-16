CREATE OR REPLACE TABLE `@PROJECT.@RAW_NATIVE_DATASET.items` AS
SELECT 
    CAST(item_nbr AS INT64)         AS item_nbr,
    CAST(family AS STRING)          AS family,
    SAFE_CAST(class AS INT64)       AS class,
    SAFE_CAST(perishable AS INT64)  AS perishable
FROM `@PROJECT.@RAW_EXT_DATASET.items_ext`

