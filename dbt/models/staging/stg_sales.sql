{{ config(materialized='view') }}

SELECT
    CAST(id AS STRING)              AS id,
    CAST(date AS DATE)              AS date,
    CAST(store_nbr AS INT64)        AS store_nbr,
    CAST(item_nbr AS INT64)         AS item_nbr,
    CAST(onpromotion AS BOOL)       AS onpromotion,
    CAST(unit_sales AS FLOAT64)     AS unit_sales
FROM {{ source('raw_native', 'sales') }}