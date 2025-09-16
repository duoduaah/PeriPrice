{{ config(
    materialized='table',
    partition_by={'field':'date', 'data_type':'date'},
    cluster_by=['store_nbr', 'item_nbr'],
    require_partition_filter=false
)
}}

WITH s AS (
    SELECT * FROM {{ ref('stg_sales') }}
)
SELECT
    id,
    date,
    store_nbr,
    item_nbr,
    COALESCE(CAST(onpromotion AS BOOL), FALSE)          AS onpromotion,  -- first non null
    LEAST(GREATEST(unit_sales, 0), 200)                 AS unit_sales,
    LOG(LEAST(GREATEST(unit_sales, 0), 200) + 1)        AS log_sales
FROM s